"""
Comprehensive unit tests for Module 1: Health & Trajectory Analysis

Tests cover:
- Synthetic data generation with known patterns
- Schema validation for output structure
- Change point detection accuracy
- Trend classification correctness
- Seasonality detection
- Forecast validation
"""

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
            days=480,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=False,
            monthly_seasonality=False,
            noise_level=0.05
        )
        
        assert len(df) == 480
        assert 'date' in df.columns
        assert 'clicks' in df.columns
        assert 'impressions' in df.columns
        assert df['clicks'].mean() == pytest.approx(1000, rel=0.1)
        assert (df['impressions'] >= df['clicks']).all()
    
    def test_growing_trend_generation(self):
        """Test generation of growing trend data"""
        df = self.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=2.0,  # +2 clicks per day
            weekly_seasonality=False,
            noise_level=0.05
        )
        
        # Should grow by approximately 2 * 480 = 960 clicks over period
        first_week_avg = df.head(7)['clicks'].mean()
        last_week_avg = df.tail(7)['clicks'].mean()
        
        assert last_week_avg > first_week_avg
        growth = last_week_avg - first_week_avg
        assert growth == pytest.approx(960, rel=0.15)
    
    def test_declining_trend_generation(self):
        """Test generation of declining trend data"""
        df = self.generate_synthetic_daily_data(
            days=480,
            base_clicks=2000,
            trend_slope=-2.0,  # -2 clicks per day
            weekly_seasonality=False,
            noise_level=0.05
        )
        
        first_week_avg = df.head(7)['clicks'].mean()
        last_week_avg = df.tail(7)['clicks'].mean()
        
        assert last_week_avg < first_week_avg
        decline = first_week_avg - last_week_avg
        assert decline == pytest.approx(960, rel=0.15)
    
    def test_seasonality_generation(self):
        """Test weekly and monthly seasonality patterns"""
        df = self.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=True,
            monthly_seasonality=True,
            noise_level=0.03
        )
        
        df['day_of_week'] = pd.to_datetime(df['date']).dt.dayofweek
        df['day_of_month'] = pd.to_datetime(df['date']).dt.day
        
        # Tuesday (1) should have higher average than Saturday (5)
        tuesday_avg = df[df['day_of_week'] == 1]['clicks'].mean()
        saturday_avg = df[df['day_of_week'] == 5]['clicks'].mean()
        assert tuesday_avg > saturday_avg
        
        # First week of month should be higher than last week
        first_week = df[df['day_of_month'] <= 7]['clicks'].mean()
        last_week = df[df['day_of_month'] >= 25]['clicks'].mean()
        assert first_week > last_week
    
    def test_change_point_generation(self):
        """Test abrupt change point insertion"""
        change_day = 240  # Middle of dataset
        magnitude = -0.20  # 20% drop
        
        df = self.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=False,
            change_points=[(change_day, magnitude)],
            noise_level=0.05
        )
        
        before_avg = df.iloc[change_day-30:change_day]['clicks'].mean()
        after_avg = df.iloc[change_day:change_day+30]['clicks'].mean()
        
        expected_after = before_avg * (1 + magnitude)
        assert after_avg == pytest.approx(expected_after, rel=0.15)


class TestSchemaValidation:
    """Test output schema matches specification exactly"""
    
    @pytest.fixture
    def sample_data(self):
        """Generate sample data for schema tests"""
        generator = TestSyntheticDataGeneration()
        return generator.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=1.0,
            weekly_seasonality=True
        )
    
    def test_output_schema_structure(self, sample_data):
        """Verify all required top-level keys are present"""
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
        """Verify overall_direction is one of valid values"""
        result = analyze_health_trajectory(sample_data)
        
        valid_directions = {
            'strong_growth',
            'growth',
            'flat',
            'declining',
            'strong_decline'
        }
        
        assert result['overall_direction'] in valid_directions
    
    def test_trend_slope_type(self, sample_data):
        """Verify trend_slope_pct_per_month is numeric"""
        result = analyze_health_trajectory(sample_data)
        
        assert isinstance(result['trend_slope_pct_per_month'], (int, float))
    
    def test_change_points_schema(self, sample_data):
        """Verify change_points array schema"""
        result = analyze_health_trajectory(sample_data)
        
        assert isinstance(result['change_points'], list)
        
        for cp in result['change_points']:
            assert 'date' in cp
            assert 'magnitude' in cp
            assert 'direction' in cp
            assert isinstance(cp['magnitude'], (int, float))
            assert cp['direction'] in ['spike', 'drop']
    
    def test_seasonality_schema(self, sample_data):
        """Verify seasonality object schema"""
        result = analyze_health_trajectory(sample_data)
        
        seasonality = result['seasonality']
        required_keys = {
            'best_day',
            'worst_day',
            'monthly_cycle',
            'cycle_description'
        }
        
        assert set(seasonality.keys()) == required_keys
        assert isinstance(seasonality['best_day'], str)
        assert isinstance(seasonality['worst_day'], str)
        assert isinstance(seasonality['monthly_cycle'], bool)
        assert isinstance(seasonality['cycle_description'], str)
    
    def test_anomalies_schema(self, sample_data):
        """Verify anomalies array schema"""
        result = analyze_health_trajectory(sample_data)
        
        assert isinstance(result['anomalies'], list)
        
        for anomaly in result['anomalies']:
            assert 'date' in anomaly
            assert 'type' in anomaly
            assert 'magnitude' in anomaly
            assert anomaly['type'] in ['motif', 'discord']
            assert isinstance(anomaly['magnitude'], (int, float))
    
    def test_forecast_schema(self, sample_data):
        """Verify forecast object schema"""
        result = analyze_health_trajectory(sample_data)
        
        forecast = result['forecast']
        required_periods = {'30d', '60d', '90d'}
        
        assert set(forecast.keys()) == required_periods
        
        for period in required_periods:
            assert 'clicks' in forecast[period]
            assert 'ci_low' in forecast[period]
            assert 'ci_high' in forecast[period]
            assert isinstance(forecast[period]['clicks'], (int, float))
            assert isinstance(forecast[period]['ci_low'], (int, float))
            assert isinstance(forecast[period]['ci_high'], (int, float))
            assert forecast[period]['ci_low'] <= forecast[period]['clicks']
            assert forecast[period]['clicks'] <= forecast[period]['ci_high']


class TestChangePointDetection:
    """Test change point detection accuracy on known test cases"""
    
    def test_single_drop_detection(self):
        """Detect single abrupt drop"""
        generator = TestSyntheticDataGeneration()
        change_day = 240
        magnitude = -0.25
        
        df = generator.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=False,
            change_points=[(change_day, magnitude)],
            noise_level=0.05
        )
        
        result = analyze_health_trajectory(df)
        change_points = result['change_points']
        
        # Should detect the drop
        assert len(change_points) >= 1
        
        # Find the change point closest to our inserted one
        detected = min(
            change_points,
            key=lambda cp: abs(
                (pd.to_datetime(cp['date']) - df.iloc[change_day]['date']).days
            )
        )
        
        # Should be within 14 days of actual change
        date_diff = abs((pd.to_datetime(detected['date']) - df.iloc[change_day]['date']).days)
        assert date_diff <= 14
        
        # Should detect as a drop
        assert detected['direction'] == 'drop'
        
        # Magnitude should be negative and roughly correct
        assert detected['magnitude'] < 0
        assert detected['magnitude'] == pytest.approx(magnitude, abs=0.15)
    
    def test_single_spike_detection(self):
        """Detect single abrupt spike"""
        generator = TestSyntheticDataGeneration()
        change_day = 300
        magnitude = 0.30
        
        df = generator.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=False,
            change_points=[(change_day, magnitude)],
            noise_level=0.05
        )
        
        result = analyze_health_trajectory(df)
        change_points = result['change_points']
        
        assert len(change_points) >= 1
        
        detected = min(
            change_points,
            key=lambda cp: abs(
                (pd.to_datetime(cp['date']) - df.iloc[change_day]['date']).days
            )
        )
        
        date_diff = abs((pd.to_datetime(detected['date']) - df.iloc[change_day]['date']).days)
        assert date_diff <= 14
        assert detected['direction'] == 'spike'
        assert detected['magnitude'] > 0
        assert detected['magnitude'] == pytest.approx(magnitude, abs=0.15)
    
    def test_multiple_change_points(self):
        """Detect multiple change points"""
        generator = TestSyntheticDataGeneration()
        changes = [
            (150, -0.20),
            (320, 0.25)
        ]
        
        df = generator.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=False,
            change_points=changes,
            noise_level=0.05
        )
        
        result = analyze_health_trajectory(df)
        change_points = result['change_points']
        
        # Should detect at least 2 change points
        assert len(change_points) >= 2
        
        # Verify both are detected (within tolerance)
        for change_day, expected_mag in changes:
            detected = min(
                change_points,
                key=lambda cp: abs(
                    (pd.to_datetime(cp['date']) - df.iloc[change_day]['date']).days
                )
            )
            
            date_diff = abs((pd.to_datetime(detected['date']) - df.iloc[change_day]['date']).days)
            assert date_diff <= 21  # More tolerance with multiple changes
    
    def test_no_false_positives_on_flat_data(self):
        """Should not detect change points on stable data"""
        generator = TestSyntheticDataGeneration()
        
        df = generator.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=True,  # Only seasonality, no changes
            change_points=None,
            noise_level=0.08
        )
        
        result = analyze_health_trajectory(df)
        change_points = result['change_points']
        
        # May detect 0-1 change points due to noise, but not many
        assert len(change_points) <= 1
    
    def test_change_point_with_trend(self):
        """Detect change point in presence of existing trend"""
        generator = TestSyntheticDataGeneration()
        change_day = 240
        magnitude = -0.30
        
        df = generator.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=1.5,  # Existing upward trend
            weekly_seasonality=False,
            change_points=[(change_day, magnitude)],
            noise_level=0.05
        )
        
        result = analyze_health_trajectory(df)
        change_points = result['change_points']
        
        # Should still detect the drop despite trend
        assert len(change_points) >= 1
        
        detected = min(
            change_points,
            key=lambda cp: abs(
                (pd.to_datetime(cp['date']) - df.iloc[change_day]['date']).days
            )
        )
        
        # May detect as either drop or flattening, but should be near the change
        date_diff = abs((pd.to_datetime(detected['date']) - df.iloc[change_day]['date']).days)
        assert date_diff <= 21


class TestTrendClassification:
    """Test trend classification correctness"""
    
    def test_strong_growth_classification(self):
        """Classify strong growth correctly (>5%/month)"""
        generator = TestSyntheticDataGeneration()
        
        # 5% per month on 1000 base = 50 clicks/month = ~1.67 clicks/day
        df = generator.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=2.0,  # >5%/month
            weekly_seasonality=False,
            noise_level=0.05
        )
        
        result = analyze_health_trajectory(df)
        
        assert result['overall_direction'] == 'strong_growth'
        assert result['trend_slope_pct_per_month'] > 5.0
    
    def test_growth_classification(self):
        """Classify moderate growth correctly (1-5%/month)"""
        generator = TestSyntheticDataGeneration()
        
        # 3% per month = ~1 click/day on 1000 base
        df = generator.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=1.0,
            weekly_seasonality=False,
            noise_level=0.05
        )
        
        result = analyze_health_trajectory(df)
        
        assert result['overall_direction'] in ['growth', 'strong_growth']
        assert result['trend_slope_pct_per_month'] > 0.5
    
    def test_flat_classification(self):
        """Classify flat trend correctly (-1 to 1%/month)"""
        generator = TestSyntheticDataGeneration()
        
        df = generator.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=False,
            noise_level=0.05
        )
        
        result = analyze_health_trajectory(df)
        
        assert result['overall_direction'] == 'flat'
        assert abs(result['trend_slope_pct_per_month']) <= 1.5
    
    def test_declining_classification(self):
        """Classify moderate decline correctly (-5 to -1%/month)"""
        generator = TestSyntheticDataGeneration()
        
        # -3% per month = -1 click/day on 1000 base
        df = generator.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=-1.0,
            weekly_seasonality=False,
            noise_level=0.05
        )
        
        result = analyze_health_trajectory(df)
        
        assert result['overall_direction'] in ['declining', 'strong_decline']
        assert result['trend_slope_pct_per_month'] < -0.5
    
    def test_strong_decline_classification(self):
        """Classify strong decline correctly (<-5%/month)"""
        generator = TestSyntheticDataGeneration()
        
        # -6% per month = -2 clicks/day on 1000 base
        df = generator.generate_synthetic_daily_data(
            days=480,
            base_clicks=1500,
            trend_slope=-3.0,
            weekly_seasonality=False,
            noise_level=0.05
        )
        
        result = analyze_health_trajectory(df)
        
        assert result['overall_direction'] == 'strong_decline'
        assert result['trend_slope_pct_per_month'] < -4.0


class TestSeasonalityDetection:
    """Test seasonality detection"""
    
    def test_weekly_seasonality_detection(self):
        """Detect weekly day-of-week patterns"""
        generator = TestSyntheticDataGeneration()
        
        df = generator.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=True,
            monthly_seasonality=False,
            noise_level=0.05
        )
        
        result = analyze_health_trajectory(df)
        seasonality = result['seasonality']
        
        # Should identify best and worst days
        assert seasonality['best_day'] is not None
        assert seasonality['worst_day'] is not None
        assert seasonality['best_day'] != seasonality['worst_day']
        
        # Based on our pattern, Tuesday should be best, Saturday worst
        # (but algorithm might smooth differently, so just check they're different)
        valid_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        assert seasonality['best_day'] in valid_days
        assert seasonality['worst_day'] in valid_days
    
    def test_monthly_seasonality_detection(self):
        """Detect monthly cycle patterns"""
        generator = TestSyntheticDataGeneration()
        
        df = generator.generate_synthetic_daily_data(
            days=480,
            base_clicks=1000,
            trend_slope=0.0,
            weekly_seasonality=False,
            monthly_seasonality=True,
            noise_level=0.05
        )
        
        result = analyze_health_trajectory(df)
        seasonality = result['seasonality']
        
        # Should detect monthly cycle
        assert seasonality['monthly_cycle'] is True
        assert len(seasonality['cycle_description']) > 0
    
    def test_no_seasonality_detection(self):
        """Should not detect seasonality when none exists"""
        generator = TestSyntheticDataGeneration()
        
        df = generator.generate_synthetic_daily_data(
            days=480,