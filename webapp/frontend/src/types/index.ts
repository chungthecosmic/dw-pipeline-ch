export interface TableInfo {
  id: string;
  name: string;
  description: string;
  partition: string;
  source: string;
}

export interface TableSchema {
  column_name: string;
  data_type: string;
}

export interface TableStats {
  record_count: number;
  min_date: string | null;
  max_date: string | null;
  partition_count: number;
}

export interface TableData {
  columns: string[];
  rows: unknown[][];
  row_count: number;
}

export interface QueryResult {
  columns: string[];
  rows: unknown[][];
  row_count: number;
}
