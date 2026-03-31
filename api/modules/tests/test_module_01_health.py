import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from api.modules.module_01_health import analyze_health_trajectory


class TestSyntheticDataGeneration:
    """Test synthetic data generation with known patterns"""
    
    def generate_synthetic_daily_data(
        self,
        days: int = 480,  # 16 months
        base_clicks: int = 1000,
        trend_slope: float = 0.0,
        weekly_seasonality: bool = True,
        monthly_seasonality: bool = False,
        change_points: list = None,
        noise_level: float = 0.1,
        random_seed: int = 42
    ) -> pd.DataFrame:
        """
        Generate synthetic daily GSC data with controllable patterns
        
        Args:
            days: Number of days to generate
            base_clicks: Baseline daily clicks
            trend_slope: Linear trend (clicks per day change)
            weekly_seasonality: Add day-of-week pattern
            monthly_seasonality: Add monthly cycle
            change_points: List of (day_index, magnitude) tuples for abrupt changes
            noise_level: Gaussian noise standard deviation as fraction of base
            random_seed: For reproducibility
        
        Returns:
            DataFrame with columns: date, clicks, impressions
        """
        np.random.seed(random_seed)
        
        start_date = datetime.now() - timedelta(days=days)
        dates = [start_date + timedelta(days=i) for i in range(days)]
        
        # Base trend
        clicks = base_clicks + trend_slope * np.arange(days)
        
        # Weekly seasonality (Tuesday high, Saturday low)
        if weekly_seasonality:
            weekly_pattern = np.array([0.95, 1.15, 1.05, 1.0, 0.98, 0.75, 0.85])  # Mon-Sun
            for i in range(days):
                day_of_week = dates[i].weekday()
                clicks[i] *= weekly_pattern[day_of_week]
        
        # Monthly seasonality (spike first week)
        if monthly_seasonality:
            for i in range(days):
                day_of_month = dates[i].day
                if day_of_month <= 7:
                    clicks[i] *= 1.15
                elif day_of_month >= 25:
                    clicks[i] *= 0.90
        
        # Change points
        if change_points:
            for day_idx, magnitude in change_points:
                if 0 <= day_idx < days:
                    clicks[day_idx:] *= (1 + magnitude)
        
        # Add noise
        noise = np.random.normal(0, base_clicks * noise_level, days)
        clicks = clicks + noise
        clicks = np.maximum(clicks, 0)  # Ensure non-negative
        
        # Impressions roughly 20x clicks with higher variance
        impressions = clicks * 20 * (1 + np.random.normal(0, 0.15, days))
        impressions = np.maximum(impressions, clicks)  # Must be >= clicks
        
        return pd.DataFrame({
            'date': dates,
            'clicks': clicks.astype(int),
            'impressions': impressions.astype(int)
        })
    
    def test_flat_trend_generation(self):
        """Test generation of flat trend data"""
        df = self.generate_synthetic_daily_data(
            days=365,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=False,
            monthly_seasonality=False,
            noise_level=0.05
        )
        
        assert len(df) == 365
        assert df['clicks'].mean() == pytest.approx(1000, rel=0.1)
        assert df['impressions'].mean() == pytest.approx(20000, rel=0.2)
    
    def test_growth_trend_generation(self):
        """Test generation of growing trend data"""
        df = self.generate_synthetic_daily_data(
            days=365,
            base_clicks=1000,
            trend_slope=2.0,  # +2 clicks per day
            weekly_seasonality=False,
            monthly_seasonality=False,
            noise_level=0.05
        )
        
        first_month_avg = df.head(30)['clicks'].mean()
        last_month_avg = df.tail(30)['clicks'].mean()
        
        # Should be ~60 clicks higher (30 days * 2 clicks/day)
        assert last_month_avg > first_month_avg
        assert last_month_avg - first_month_avg == pytest.approx(60 * 11, rel=0.2)
    
    def test_weekly_seasonality_generation(self):
        """Test weekly seasonality pattern"""
        df = self.generate_synthetic_daily_data(
            days=365,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=True,
            monthly_seasonality=False,
            noise_level=0.05
        )
        
        df['day_of_week'] = pd.to_datetime(df['date']).dt.dayofweek
        daily_avgs = df.groupby('day_of_week')['clicks'].mean()
        
        # Tuesday (1) should be highest, Saturday (5) should be lowest
        assert daily_avgs.idxmax() == 1
        assert daily_avgs.idxmin() == 5
    
    def test_change_point_generation(self):
        """Test abrupt change point insertion"""
        change_day = 200
        magnitude = -0.3  # 30% drop
        
        df = self.generate_synthetic_daily_data(
            days=400,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=False,
            monthly_seasonality=False,
            change_points=[(change_day, magnitude)],
            noise_level=0.05
        )
        
        before_avg = df.iloc[change_day-30:change_day]['clicks'].mean()
        after_avg = df.iloc[change_day:change_day+30]['clicks'].mean()
        
        # After should be ~30% lower
        assert after_avg < before_avg
        assert after_avg / before_avg == pytest.approx(0.7, rel=0.15)


class TestSchemaValidation:
    """Test output schema structure and completeness"""
    
    @pytest.fixture
    def sample_data(self):
        """Generate standard test dataset"""
        gen = TestSyntheticDataGeneration()
        return gen.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=1.0,
            weekly_seasonality=True,
            monthly_seasonality=True
        )
    
    def test_output_structure(self, sample_data):
        """Test that output has all required top-level keys"""
        result = analyze_health_trajectory(sample_data)
        
        required_keys = {
            'overall_direction',
            'trend_slope_pct_per_month',
            'change_points',
            'seasonality',
            'anomalies',
            'forecast'
        }
        
        assert set(result.keys()) == required_keys
    
    def test_overall_direction_values(self, sample_data):
        """Test overall_direction is valid enum value"""
        result = analyze_health_trajectory(sample_data)
        
        valid_directions = {
            'strong_growth',
            'growth',
            'flat',
            'decline',
            'strong_decline'
        }
        
        assert result['overall_direction'] in valid_directions
    
    def test_trend_slope_type(self, sample_data):
        """Test trend_slope_pct_per_month is numeric"""
        result = analyze_health_trajectory(sample_data)
        
        assert isinstance(result['trend_slope_pct_per_month'], (int, float))
    
    def test_change_points_structure(self, sample_data):
        """Test change_points array has correct structure"""
        result = analyze_health_trajectory(sample_data)
        
        assert isinstance(result['change_points'], list)
        
        for cp in result['change_points']:
            assert 'date' in cp
            assert 'magnitude' in cp
            assert 'direction' in cp
            assert isinstance(cp['magnitude'], (int, float))
            assert cp['direction'] in ['drop', 'spike']
    
    def test_seasonality_structure(self, sample_data):
        """Test seasonality object has required fields"""
        result = analyze_health_trajectory(sample_data)
        
        seasonality = result['seasonality']
        assert 'best_day' in seasonality
        assert 'worst_day' in seasonality
        assert 'monthly_cycle' in seasonality
        
        assert isinstance(seasonality['monthly_cycle'], bool)
        
        if seasonality.get('cycle_description'):
            assert isinstance(seasonality['cycle_description'], str)
    
    def test_forecast_structure(self, sample_data):
        """Test forecast has all time horizons with confidence intervals"""
        result = analyze_health_trajectory(sample_data)
        
        forecast = result['forecast']
        required_horizons = ['30d', '60d', '90d']
        
        for horizon in required_horizons:
            assert horizon in forecast
            assert 'clicks' in forecast[horizon]
            assert 'ci_low' in forecast[horizon]
            assert 'ci_high' in forecast[horizon]
            
            # Confidence intervals should make sense
            assert forecast[horizon]['ci_low'] <= forecast[horizon]['clicks']
            assert forecast[horizon]['clicks'] <= forecast[horizon]['ci_high']


class TestChangePointDetection:
    """Test accuracy of change point detection"""
    
    def test_single_drop_detection(self):
        """Test detection of single abrupt drop"""
        gen = TestSyntheticDataGeneration()
        change_day = 240
        magnitude = -0.25
        
        df = gen.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=False,
            change_points=[(change_day, magnitude)],
            noise_level=0.05
        )
        
        result = analyze_health_trajectory(df)
        
        # Should detect at least one change point
        assert len(result['change_points']) >= 1
        
        # Should detect the intentional drop
        detected_dates = [pd.to_datetime(cp['date']) for cp in result['change_points']]
        change_date = df.iloc[change_day]['date']
        
        # Allow ±7 day window for detection
        date_diffs = [abs((d - pd.to_datetime(change_date)).days) for d in detected_dates]
        assert min(date_diffs) <= 7, "Change point not detected within 7-day window"
    
    def test_multiple_change_detection(self):
        """Test detection of multiple change points"""
        gen = TestSyntheticDataGeneration()
        
        df = gen.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=False,
            change_points=[
                (120, -0.20),  # Drop at 4 months
                (360, 0.30)     # Spike at 12 months
            ],
            noise_level=0.05
        )
        
        result = analyze_health_trajectory(df)
        
        # Should detect at least 2 change points
        assert len(result['change_points']) >= 2
        
        # Should have both drop and spike
        directions = [cp['direction'] for cp in result['change_points']]
        assert 'drop' in directions
        assert 'spike' in directions
    
    def test_no_false_positives_flat_trend(self):
        """Test that flat trend doesn't generate spurious change points"""
        gen = TestSyntheticDataGeneration()
        
        df = gen.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=True,
            monthly_seasonality=False,
            change_points=None,
            noise_level=0.08
        )
        
        result = analyze_health_trajectory(df)
        
        # Should have 0-2 change points max (some noise-induced false positives acceptable)
        assert len(result['change_points']) <= 2


class TestTrendClassification:
    """Test trend direction classification accuracy"""
    
    def test_strong_growth_classification(self):
        """Test strong growth (>5%/month) classification"""
        gen = TestSyntheticDataGeneration()
        
        # 5% monthly growth = ~1.67 clicks/day on base 1000
        df = gen.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=1.8,
            weekly_seasonality=True,
            noise_level=0.05
        )
        
        result = analyze_health_trajectory(df)
        
        assert result['overall_direction'] in ['strong_growth', 'growth']
        assert result['trend_slope_pct_per_month'] > 4.0
    
    def test_decline_classification(self):
        """Test decline classification"""
        gen = TestSyntheticDataGeneration()
        
        # -2% monthly decline
        df = gen.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=-0.7,
            weekly_seasonality=True,
            noise_level=0.05
        )
        
        result = analyze_health_trajectory(df)
        
        assert result['overall_direction'] in ['decline', 'strong_decline']
        assert result['trend_slope_pct_per_month'] < -1.0
    
    def test_flat_classification(self):
        """Test flat trend classification"""
        gen = TestSyntheticDataGeneration()
        
        df = gen.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=True,
            noise_level=0.05
        )
        
        result = analyze_health_trajectory(df)
        
        assert result['overall_direction'] == 'flat'
        assert abs(result['trend_slope_pct_per_month']) < 1.0


class TestSeasonalityDetection:
    """Test seasonality detection and characterization"""
    
    def test_weekly_seasonality_detection(self):
        """Test detection of weekly seasonality pattern"""
        gen = TestSyntheticDataGeneration()
        
        df = gen.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=True,
            monthly_seasonality=False,
            noise_level=0.05
        )
        
        result = analyze_health_trajectory(df)
        
        seasonality = result['seasonality']
        assert seasonality['best_day'] is not None
        assert seasonality['worst_day'] is not None
        
        # Best should be Tuesday, worst should be Saturday
        assert seasonality['best_day'] in ['Monday', 'Tuesday', 'Wednesday']
        assert seasonality['worst_day'] in ['Friday', 'Saturday', 'Sunday']
    
    def test_monthly_cycle_detection(self):
        """Test detection of monthly cycle"""
        gen = TestSyntheticDataGeneration()
        
        df = gen.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=False,
            monthly_seasonality=True,
            noise_level=0.05
        )
        
        result = analyze_health_trajectory(df)
        
        seasonality = result['seasonality']
        assert seasonality['monthly_cycle'] == True
        assert 'cycle_description' in seasonality
    
    def test_no_seasonality_detection(self):
        """Test that random data doesn't produce false seasonality"""
        gen = TestSyntheticDataGeneration()
        
        df = gen.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=False,
            monthly_seasonality=False,
            noise_level=0.15
        )
        
        result = analyze_health_trajectory(df)
        
        seasonality = result['seasonality']
        # Should either have no strong day preference or monthly_cycle = False
        assert seasonality['monthly_cycle'] == False


class TestForecastValidation:
    """Test forecast generation and reasonableness"""
    
    def test_forecast_continuation_of_trend(self):
        """Test that forecast continues detected trend"""
        gen = TestSyntheticDataGeneration()
        
        # Strong upward trend
        df = gen.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=2.0,
            weekly_seasonality=False,
            noise_level=0.05
        )
        
        result = analyze_health_trajectory(df)
        
        current_avg = df.tail(30)['clicks'].mean()
        forecast_30d = result['forecast']['30d']['clicks']
        forecast_90d = result['forecast']['90d']['clicks']
        
        # Forecast should be higher than current (growing trend)
        assert forecast_30d > current_avg * 0.9  # Allow some deceleration
        assert forecast_90d > forecast_30d * 0.9
    
    def test_forecast_confidence_intervals_widen(self):
        """Test that confidence intervals widen over time"""
        gen = TestSyntheticDataGeneration()
        
        df = gen.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=1.0,
            weekly_seasonality=True,
            noise_level=0.1
        )
        
        result = analyze_health_trajectory(df)
        
        forecast = result['forecast']
        
        ci_30d = forecast['30d']['ci_high'] - forecast['30d']['ci_low']
        ci_60d = forecast['60d']['ci_high'] - forecast['60d']['ci_low']
        ci_90d = forecast['90d']['ci_high'] - forecast['90d']['ci_low']
        
        # Confidence intervals should widen
        assert ci_60d > ci_30d
        assert ci_90d > ci_60d
    
    def test_forecast_positive_values(self):
        """Test that forecast never produces negative clicks"""
        gen = TestSyntheticDataGeneration()
        
        # Strong decline
        df = gen.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=-2.0,
            weekly_seasonality=False,
            noise_level=0.05
        )
        
        result = analyze_health_trajectory(df)
        
        forecast = result['forecast']
        
        for horizon in ['30d', '60d', '90d']:
            assert forecast[horizon]['clicks'] >= 0
            assert forecast[horizon]['ci_low'] >= 0
            assert forecast[horizon]['ci_high'] >= 0


class TestAnomalyDetection:
    """Test anomaly detection functionality"""
    
    def test_anomaly_detection_on_spike(self):
        """Test detection of one-off traffic spike"""
        gen = TestSyntheticDataGeneration()
        
        df = gen.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=False,
            noise_level=0.05
        )
        
        # Inject anomalous spike
        spike_day = 200
        df.loc[spike_day, 'clicks'] = df.loc[spike_day, 'clicks'] * 3
        
        result = analyze_health_trajectory(df)
        
        anomalies = result['anomalies']
        
        # Should detect at least one anomaly
        assert len(anomalies) >= 1
        
        # Check if spike day is detected
        if len(anomalies) > 0:
            anomaly_dates = [pd.to_datetime(a['date']) for a in anomalies]
            spike_date = df.iloc[spike_day]['date']
            date_diffs = [abs((d - pd.to_datetime(spike_date)).days) for d in anomaly_dates]
            assert min(date_diffs) <= 3


class TestEdgeCases:
    """Test edge cases and error handling"""
    
    def test_minimum_data_requirement(self):
        """Test behavior with minimal data (30 days)"""
        gen = TestSyntheticDataGeneration()
        
        df = gen.generate_synthetic_daily_data(
            days=30,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=False
        )
        
        result = analyze_health_trajectory(df)
        
        # Should still return valid structure
        assert 'overall_direction' in result
        assert 'forecast' in result
        
        # But may have degraded quality (no change points expected)
        assert len(result['change_points']) == 0
    
    def test_zero_clicks_handling(self):
        """Test handling of days with zero clicks"""
        gen = TestSyntheticDataGeneration()
        
        df = gen.generate_synthetic_daily_data(
            days=480,
            base_clicks=100,
            trend_slope=0.0,
            noise_level=0.3
        )
        
        # Force some zero days
        df.loc[df['clicks'] < 10, 'clicks'] = 0
        
        result = analyze_health_trajectory(df)
        
        # Should handle gracefully
        assert result is not None
        assert 'overall_direction' in result
    
    def test_missing_dates_interpolation(self):
        """Test handling of missing dates in sequence"""
        gen = TestSyntheticDataGeneration()
        
        df = gen.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000
        )
        
        # Remove random 10% of rows
        df = df.sample(frac=0.9).sort_values('date').reset_index(drop=True)
        
        result = analyze_health_trajectory(df)
        
        # Should handle gracefully
        assert result is not None
        assert 'overall_direction' in result


class TestIntegrationScenarios:
    """Test realistic end-to-end scenarios"""
    
    def test_algorithm_update_scenario(self):
        """Test scenario mimicking algorithm update impact"""
        gen = TestSyntheticDataGeneration()
        
        df = gen.generate_synthetic_daily_data(
            days=480,
            base_clicks=5000,
            trend_slope=2.0,  # Growing before update
            weekly_seasonality=True,
            change_points=[
                (300, -0.35)  # 35% drop at ~10 months (algorithm update)
            ],
            noise_level=0.08
        )
        
        result = analyze_health_trajectory(df)
        
        # Should classify as declining overall (recent drop overrides earlier growth)
        assert result['overall_direction'] in ['decline', 'strong_decline']
        
        # Should detect the major change point
        assert len(result['change_points']) >= 1
        
        major_drops = [cp for cp in result['change_points'] if cp['direction'] == 'drop']
        assert len(major_drops) >= 1
        
        # Magnitude should be significant
        assert any(abs(cp['magnitude']) > 0.25 for cp in major_drops)
    
    def test_seasonal_business_scenario(self):
        """Test scenario with strong seasonal pattern"""
        gen = TestSyntheticDataGeneration()
        
        df = gen.generate_synthetic_daily_data(
            days=480,
            base_clicks=2000,
            trend_slope=0.5,
            weekly_seasonality=True,
            monthly_seasonality=True,
            noise_level=0.1
        )
        
        result = analyze_health_trajectory(df)
        
        # Should detect monthly cycle
        assert result['seasonality']['monthly_cycle'] == True
        
        # Should still identify underlying growth trend
        assert result['overall_direction'] in ['growth', 'strong_growth', 'flat']
    
    def test_recovery_scenario(self):
        """Test scenario with drop and recovery"""
        gen = TestSyntheticDataGeneration()
        
        df = gen.generate_synthetic_daily_data(
            days=480,
            base_clicks=3000,
            trend_slope=0.0,
            weekly_seasonality=True,
            change_points=[
                (200, -0.30),  # Drop
                (350, 0.40)    # Recovery
            ],
            noise_level=0.08
        )
        
        result = analyze_health_trajectory(df)
        
        # Should detect both change points
        assert len(result['change_points']) >= 2
        
        # Should have both drops and spikes
        directions = [cp['direction'] for cp in result['change_points']]
        assert 'drop' in directions
        assert 'spike' in directions
        
        # Overall direction depends on final trend
        assert result['overall_direction'] in ['flat', 'growth', 'decline']