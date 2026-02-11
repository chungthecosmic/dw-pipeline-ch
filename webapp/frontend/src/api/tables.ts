import axios from 'axios';
import type { TableInfo, TableSchema, TableStats, TableData, QueryResult } from '../types';

// Docker 환경에서는 프록시를 통해, 로컬 개발에서는 직접 접근
const API_BASE = '/api/tables';

const api = axios.create({
  baseURL: API_BASE,
  timeout: 30000,
});

export async function fetchTables(): Promise<TableInfo[]> {
  const response = await api.get<TableInfo[]>('');
  return response.data;
}

export async function fetchTableInfo(tableId: string): Promise<TableInfo> {
  const response = await api.get<TableInfo>(`/${tableId}`);
  return response.data;
}

export async function fetchTableSchema(tableId: string): Promise<TableSchema[]> {
  const response = await api.get<TableSchema[]>(`/${tableId}/schema`);
  return response.data;
}

export async function fetchTableStats(tableId: string): Promise<TableStats> {
  const response = await api.get<TableStats>(`/${tableId}/stats`);
  return response.data;
}

export async function fetchTableData(
  tableId: string,
  limit: number = 100,
  offset: number = 0
): Promise<TableData> {
  const response = await api.get<TableData>(`/${tableId}/data`, {
    params: { limit, offset },
  });
  return response.data;
}

export async function executeQuery(sql: string): Promise<QueryResult> {
  const response = await api.post<QueryResult>('/query', { sql });
  return response.data;
}
