/**
 * Module 5: Gameplan
 * TypeScript interfaces matching the spec output schema
 */

export type EffortLevel = 'low' | 'medium' | 'high';

export type ActionCategory = 'critical' | 'quick_wins' | 'strategic' | 'structural';

export interface GameplanAction {
  /** Specific page or keyword affected */
  page?: string;
  keyword?: string;
  /** Concrete instruction on what to do */
  action: string;
  /** Estimated traffic impact in clicks per month */
  impact: number;
  /** Effort level required */
  effort: EffortLevel;
  /** Optional dependencies on other actions */
  dependencies?: string[];
  /** Additional context or reasoning */
  reason?: string;
}

export interface GameplanCategory {
  /** List of actions in this category */
  actions: GameplanAction[];
  /** Category-specific summary metrics */
  total_impact?: number;
  /** Category description */
  description?: string;
}

export interface GameplanOutput {
  /** Critical fixes (do this week) */
  critical: GameplanAction[];
  /** Quick wins (do this month) */
  quick_wins: GameplanAction[];
  /** Strategic plays (this quarter) */
  strategic: GameplanAction[];
  /** Structural improvements (ongoing) */
  structural: GameplanAction[];
  /** Total estimated monthly click recovery across all actions */
  total_estimated_monthly_click_recovery: number;
  /** Total estimated monthly click growth from new opportunities */
  total_estimated_monthly_click_growth: number;
  /** Human-readable narrative synthesized by LLM */
  narrative: string;
  /** Optional: breakdown by category with richer metadata */
  categories?: {
    critical: GameplanCategory;
    quick_wins: GameplanCategory;
    strategic: GameplanCategory;
    structural: GameplanCategory;
  };
}

/**
 * Extended action interface with all possible fields from spec examples
 */
export interface DetailedGameplanAction extends GameplanAction {
  /** URL of the affected page */
  url?: string;
  /** Current monthly clicks for this page/keyword */
  current_monthly_clicks?: number;
  /** Projected recovery date or timeline */
  timeline?: string;
  /** Specific recommendation type (from Module 2 triage) */
  recommended_action?: 'title_rewrite' | 'content_expansion' | 'consolidate' | 'differentiate' | 'canonical_redirect' | 'internal_links' | 'schema_markup' | 'video_creation' | 'faq_schema';
  /** Priority score (from Module 2 or synthesis logic) */
  priority_score?: number;
  /** Whether this is a CTR anomaly fix */
  ctr_anomaly?: boolean;
  /** Expected vs actual CTR (if CTR fix) */
  ctr_expected?: number;
  ctr_actual?: number;
  /** SERP feature opportunity type (if applicable) */
  serp_feature?: string;
  /** Estimated difficulty of capturing feature */
  difficulty?: 'low' | 'medium' | 'high';
  /** Cannibalization cluster info (if consolidation action) */
  cannibalizing_pages?: string[];
  /** Content age category (if content refresh) */
  content_age_quadrant?: 'urgent_update' | 'leave_alone' | 'structural_problem' | 'double_down';
  /** Algorithm update related (if applicable) */
  algorithm_related?: boolean;
  update_name?: string;
  /** Authority flow improvement (if architecture action) */
  pagerank_boost?: number;
  /** Revenue impact (if Module 12 data available) */
  revenue_impact?: number;
}

/**
 * Gameplan metadata for tracking generation
 */
export interface GameplanMetadata {
  /** Timestamp of gameplan generation */
  generated_at: string;
  /** Version of gameplan logic used */
  version: string;
  /** Input module statuses */
  input_modules: {
    health: boolean;
    triage: boolean;
    serp: boolean;
    content: boolean;
    algorithm?: boolean;
    architecture?: boolean;
    revenue?: boolean;
  };
  /** LLM model used for narrative */
  llm_model?: string;
  /** Total actions across all categories */
  total_actions: number;
}

/**
 * Complete Module 5 output with metadata
 */
export interface Module5Output {
  gameplan: GameplanOutput;
  metadata: GameplanMetadata;
}

/**
 * Helper type for effort badge display
 */
export interface EffortBadge {
  level: EffortLevel;
  label: string;
  color: string;
}

export const EFFORT_BADGES: Record<EffortLevel, EffortBadge> = {
  low: {
    level: 'low',
    label: 'Low effort',
    color: 'green'
  },
  medium: {
    level: 'medium',
    label: 'Medium effort',
    color: 'yellow'
  },
  high: {
    level: 'high',
    label: 'High effort',
    color: 'red'
  }
};

/**
 * Category display configuration
 */
export interface CategoryConfig {
  key: ActionCategory;
  title: string;
  description: string;
  icon: string;
  timeframe: string;
}

export const CATEGORY_CONFIGS: Record<ActionCategory, CategoryConfig> = {
  critical: {
    key: 'critical',
    title: 'Critical Fixes',
    description: 'High-impact actions that need immediate attention',
    icon: 'alert-circle',
    timeframe: 'This week'
  },
  quick_wins: {
    key: 'quick_wins',
    title: 'Quick Wins',
    description: 'Low-effort, high-return optimizations',
    icon: 'zap',
    timeframe: 'This month'
  },
  strategic: {
    key: 'strategic',
    title: 'Strategic Plays',
    description: 'Medium to long-term initiatives for sustained growth',
    icon: 'target',
    timeframe: 'This quarter'
  },
  structural: {
    key: 'structural',
    title: 'Structural Improvements',
    description: 'Foundational changes for long-term competitive advantage',
    icon: 'layers',
    timeframe: 'Ongoing'
  }
};
