import axios from 'axios';
import type { TableInfo, TableSchema, TableStats, TableData, QueryResult } from '../types';

const API_BASE = '/api/tables';

export async function fetchTables(): Promise<TableInfo[]> {
  const response = await axios.get<TableInfo[]>(API_BASE);
  return response.data;
}

export async function fetchTableInfo(tableId: string): Promise<TableInfo> {
  const response = await axios.get<TableInfo>(`${API_BASE}/${tableId}`);
  return response.data;
}

export async function fetchTableSchema(tableId: string): Promise<TableSchema[]> {
  const response = await axios.get<TableSchema[]>(`${API_BASE}/${tableId}/schema`);
  return response.data;
}

export async function fetchTableStats(tableId: string): Promise<TableStats> {
  const response = await axios.get<TableStats>(`${API_BASE}/${tableId}/stats`);
  return response.data;
}

export async function fetchTableData(
  tableId: string,
  limit: number = 100,
  offset: number = 0
): Promise<TableData> {
  const response = await axios.get<TableData>(`${API_BASE}/${tableId}/data`, {
    params: { limit, offset },
  });
  return response.data;
}

export async function executeQuery(sql: string): Promise<QueryResult> {
  const response = await axios.post<QueryResult>(`${API_BASE}/query`, { sql });
  return response.data;
}
