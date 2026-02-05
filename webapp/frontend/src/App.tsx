import { useState, useEffect } from 'react';
import { fetchTables, fetchTableSchema, fetchTableStats, fetchTableData, executeQuery } from './api/tables';
import type { TableInfo, TableSchema, TableStats, TableData, QueryResult } from './types';

type TabType = 'info' | 'schema' | 'data' | 'query';

function App() {
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [selectedTable, setSelectedTable] = useState<TableInfo | null>(null);
  const [activeTab, setActiveTab] = useState<TabType>('info');
  const [schema, setSchema] = useState<TableSchema[]>([]);
  const [stats, setStats] = useState<TableStats | null>(null);
  const [data, setData] = useState<TableData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // SQL Query state
  const [sql, setSql] = useState('');
  const [queryResult, setQueryResult] = useState<QueryResult | null>(null);
  const [queryLoading, setQueryLoading] = useState(false);
  const [queryError, setQueryError] = useState<string | null>(null);

  useEffect(() => {
    loadTables();
  }, []);

  useEffect(() => {
    if (selectedTable) {
      loadTableDetails(selectedTable.id);
    }
  }, [selectedTable, activeTab]);

  async function loadTables() {
    try {
      const data = await fetchTables();
      setTables(data);
      if (data.length > 0) {
        setSelectedTable(data[0]);
      }
    } catch (err) {
      setError('테이블 목록을 불러오는데 실패했습니다.');
      console.error(err);
    }
  }

  async function loadTableDetails(tableId: string) {
    setLoading(true);
    setError(null);
    try {
      if (activeTab === 'info' || activeTab === 'schema') {
        const [schemaData, statsData] = await Promise.all([
          fetchTableSchema(tableId),
          fetchTableStats(tableId),
        ]);
        setSchema(schemaData);
        setStats(statsData);
      } else if (activeTab === 'data') {
        const tableData = await fetchTableData(tableId);
        setData(tableData);
      }
    } catch (err) {
      setError('데이터를 불러오는데 실패했습니다.');
      console.error(err);
    } finally {
      setLoading(false);
    }
  }

  async function handleQueryExecute() {
    if (!sql.trim()) return;

    setQueryLoading(true);
    setQueryError(null);
    setQueryResult(null);

    try {
      const result = await executeQuery(sql);
      setQueryResult(result);
    } catch (err: unknown) {
      const error = err as { response?: { data?: { detail?: string } }; message?: string };
      setQueryError(error.response?.data?.detail || error.message || '쿼리 실행에 실패했습니다.');
    } finally {
      setQueryLoading(false);
    }
  }

  function formatValue(value: unknown): string {
    if (value === null || value === undefined) return '-';
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
  }

  return (
    <div className="container">
      <header className="header">
        <h1>DW Pipeline 데이터 카탈로그</h1>
        <p>Iceberg 테이블 스키마 및 데이터 조회</p>
      </header>

      <div className="main-content">
        <aside className="sidebar">
          <h2>테이블 목록</h2>
          <ul className="table-list">
            {tables.map((table) => (
              <li
                key={table.id}
                className={`table-item ${selectedTable?.id === table.id ? 'active' : ''}`}
                onClick={() => setSelectedTable(table)}
              >
                <h3>{table.name}</h3>
                <p>{table.id}</p>
              </li>
            ))}
          </ul>
        </aside>

        <main className="content-area">
          {selectedTable ? (
            <>
              <div className="tabs">
                <button
                  className={`tab ${activeTab === 'info' ? 'active' : ''}`}
                  onClick={() => setActiveTab('info')}
                >
                  정보
                </button>
                <button
                  className={`tab ${activeTab === 'schema' ? 'active' : ''}`}
                  onClick={() => setActiveTab('schema')}
                >
                  스키마
                </button>
                <button
                  className={`tab ${activeTab === 'data' ? 'active' : ''}`}
                  onClick={() => setActiveTab('data')}
                >
                  데이터
                </button>
                <button
                  className={`tab ${activeTab === 'query' ? 'active' : ''}`}
                  onClick={() => setActiveTab('query')}
                >
                  SQL 쿼리
                </button>
              </div>

              {error && <div className="error">{error}</div>}

              {activeTab === 'info' && (
                <div>
                  <div className="info-card">
                    <h3>{selectedTable.name}</h3>
                    <p>{selectedTable.description}</p>
                    <p>
                      <span className="badge">파티션: {selectedTable.partition}</span>
                      <span className="badge">소스: {selectedTable.source}</span>
                    </p>
                  </div>

                  {loading ? (
                    <div className="loading">로딩 중...</div>
                  ) : stats && (
                    <div className="stats-grid">
                      <div className="stat-card">
                        <div className="value">{stats.record_count.toLocaleString()}</div>
                        <div className="label">총 레코드 수</div>
                      </div>
                      <div className="stat-card">
                        <div className="value">{stats.partition_count.toLocaleString()}</div>
                        <div className="label">파티션 수</div>
                      </div>
                      <div className="stat-card">
                        <div className="value">{stats.min_date || '-'}</div>
                        <div className="label">최소 날짜</div>
                      </div>
                      <div className="stat-card">
                        <div className="value">{stats.max_date || '-'}</div>
                        <div className="label">최대 날짜</div>
                      </div>
                    </div>
                  )}
                </div>
              )}

              {activeTab === 'schema' && (
                <div>
                  {loading ? (
                    <div className="loading">로딩 중...</div>
                  ) : (
                    <table className="schema-table">
                      <thead>
                        <tr>
                          <th>컬럼명</th>
                          <th>데이터 타입</th>
                        </tr>
                      </thead>
                      <tbody>
                        {schema.map((col, idx) => (
                          <tr key={idx}>
                            <td>{col.column_name}</td>
                            <td><span className="data-type">{col.data_type}</span></td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>
              )}

              {activeTab === 'data' && (
                <div>
                  {loading ? (
                    <div className="loading">로딩 중...</div>
                  ) : data ? (
                    <>
                      <p style={{ marginBottom: '15px', color: '#666' }}>
                        {data.row_count}개 행 표시
                      </p>
                      <div className="data-table-wrapper">
                        <table className="data-table">
                          <thead>
                            <tr>
                              {data.columns.map((col, idx) => (
                                <th key={idx}>{col}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {data.rows.map((row, rowIdx) => (
                              <tr key={rowIdx}>
                                {row.map((cell, cellIdx) => (
                                  <td key={cellIdx}>{formatValue(cell)}</td>
                                ))}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </>
                  ) : (
                    <div className="empty-state">
                      <h3>데이터 없음</h3>
                      <p>테이블에 데이터가 없습니다.</p>
                    </div>
                  )}
                </div>
              )}

              {activeTab === 'query' && (
                <div>
                  <p style={{ marginBottom: '10px', color: '#666', fontSize: '0.9rem' }}>
                    SELECT 쿼리만 실행 가능합니다. iceberg_scan() 함수를 사용하세요.
                  </p>
                  <textarea
                    className="sql-editor"
                    value={sql}
                    onChange={(e) => setSql(e.target.value)}
                    placeholder={`예시:\nSELECT * FROM iceberg_scan('s3://dw-pipeline-ch/warehouse/stock_data/${selectedTable.id}') LIMIT 10`}
                  />
                  <button
                    className="btn btn-primary"
                    onClick={handleQueryExecute}
                    disabled={queryLoading || !sql.trim()}
                  >
                    {queryLoading ? '실행 중...' : '쿼리 실행'}
                  </button>

                  {queryError && (
                    <div className="error" style={{ marginTop: '15px' }}>
                      {queryError}
                    </div>
                  )}

                  {queryResult && (
                    <div style={{ marginTop: '20px' }}>
                      <p style={{ marginBottom: '10px', color: '#666' }}>
                        결과: {queryResult.row_count}개 행
                      </p>
                      <div className="data-table-wrapper">
                        <table className="data-table">
                          <thead>
                            <tr>
                              {queryResult.columns.map((col, idx) => (
                                <th key={idx}>{col}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {queryResult.rows.map((row, rowIdx) => (
                              <tr key={rowIdx}>
                                {row.map((cell, cellIdx) => (
                                  <td key={cellIdx}>{formatValue(cell)}</td>
                                ))}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  )}
                </div>
              )}
            </>
          ) : (
            <div className="empty-state">
              <h3>테이블을 선택하세요</h3>
              <p>왼쪽 목록에서 테이블을 선택하면 상세 정보를 확인할 수 있습니다.</p>
            </div>
          )}
        </main>
      </div>
    </div>
  );
}

export default App;
