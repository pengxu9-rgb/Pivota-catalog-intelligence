export type RowStatus = "EMPTY" | "OK" | "PENDING" | "NEEDS_SOURCE" | "SKIPPED" | "ERROR";
export type TaskStatus = "RUNNING" | "COMPLETED" | "FAILED" | "CANCELED";
export type ParseStatus = "OK" | "NEEDS_SOURCE" | "NEEDS_REVIEW";
export type ReviewStatus = "UNREVIEWED" | "APPROVED" | "REJECTED" | "NEEDS_SOURCE" | "NEEDS_REVIEW";
export type AuditStatus = "UNAUDITED" | "PASS" | "REVIEW" | "FAIL";
export type SourceMatchStatus = "missing" | "match" | "mismatch" | "unknown";
export type IngredientSignalType = "none" | "structured_list" | "labeled_ingredients" | "noisy_text" | "ambiguous_text";

export type ParsedIngredient = {
  order: number;
  standard_name: string;
  original_text: string;
  uncertain?: boolean;
  needs_review?: boolean;
};

export type ImportResponse = {
  import_id: string;
  filename: string;
  created_at: string;
  total_rows: number;
};

export type CandidateRow = {
  row_id: string;
  row_index: number;
  brand: string;
  product_name: string;
  market: string;
  candidate_id?: string | null;
  sku_key?: string | null;
  external_seed_id?: string | null;
  external_product_id?: string | null;
  status: RowStatus;
  confidence: number | null;
  source_type: string | null;
  source_ref: string | null;
  raw_ingredient_text: string | null;
  cleaned_text?: string | null;
  parse_status?: ParseStatus | null;
  parse_confidence?: number | null;
  inci_list?: string | null;
  inci_list_json: ParsedIngredient[];
  unrecognized_tokens: string[];
  normalization_notes: string[];
  needs_review: Array<Record<string, any>>;
  review_status: ReviewStatus;
  reviewed_by?: string | null;
  reviewed_at?: string | null;
  audit_status: AuditStatus;
  audit_score?: number | null;
  source_match_status?: SourceMatchStatus | null;
  ingredient_signal_type?: IngredientSignalType | null;
  ingest_allowed: boolean;
  updated_at: string;
  error: string | null;
};

export type ListRowsResponse = {
  import_id: string;
  total: number;
  items: CandidateRow[];
};

export type CreateTaskResponse = {
  task_id: string;
  import_id: string;
  status: TaskStatus;
  queued: number;
};

export type TaskProgress = {
  task_id: string;
  import_id: string;
  status: TaskStatus;
  force: boolean;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  counts: Record<string, number>;
};

export type ImportAuditResponse = {
  import_id: string;
  audit_run_id: string;
  scanned: number;
  flagged_rows: number;
  findings_total: number;
  blocker_count: number;
  review_count: number;
  info_count: number;
  corrected_rows: number;
};

export type ReviewUpdateRequest = {
  raw_ingredient_text?: string | null;
  source_ref?: string | null;
  source_type?: string | null;
  cleaned_text?: string | null;
  parse_status?: ParseStatus | null;
  parse_confidence?: number | null;
  inci_list?: string | null;
  inci_list_json?: ParsedIngredient[];
  unrecognized_tokens?: string[];
  normalization_notes?: string[];
  needs_review?: Array<Record<string, any>>;
  review_status: ReviewStatus;
  reviewed_by?: string | null;
};

export type CorrectionUpdateRequest = {
  correction_type: string;
  actor?: string | null;
  raw_ingredient_text?: string | null;
  source_ref?: string | null;
  source_type?: string | null;
  brand?: string | null;
  product_name?: string | null;
  market?: string | null;
  apply_parser?: boolean;
};
