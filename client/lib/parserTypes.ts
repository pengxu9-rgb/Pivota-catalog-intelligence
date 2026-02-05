export type ParseStatus = "OK" | "NEEDS_SOURCE" | "NEEDS_REVIEW";

export type ParsedIngredient = {
  order: number;
  standard_name: string;
  original_text: string;
  uncertain?: boolean;
  needs_review?: boolean;
};

export type ParserReparseResponse = {
  cleaned_text: string;
  parse_status: ParseStatus;
  inci_list: string;
  inci_list_json: ParsedIngredient[];
  unrecognized_tokens: string[];
  normalization_notes: string[];
  parse_confidence: number;
  needs_review: Array<Record<string, any>>;
};

export type ParserReparseBatchRequest = {
  items: Array<{ row_id: string; raw_ingredient_text?: string | null }>;
};

export type ParserReparseBatchResponse = {
  items: Array<{ row_id: string; result: ParserReparseResponse }>;
};

