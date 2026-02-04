export type RowStatus = "EMPTY" | "OK" | "PENDING" | "NEEDS_SOURCE" | "SKIPPED" | "ERROR";
export type TaskStatus = "RUNNING" | "COMPLETED" | "FAILED" | "CANCELED";

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
  status: RowStatus;
  confidence: number | null;
  source_type: string | null;
  source_ref: string | null;
  raw_ingredient_text: string | null;
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

