import { useState, useEffect } from 'react';
import { fetchTables, fetchTableSchema, fetchTableStats, fetchTableData, executeQuery } from './api/tables';
import type { TableInfo, TableSchema, TableStats, TableData, QueryResult } from './types';

type TabType = 'info' | 'schema' | 'data' | 'query';

const TABLE_ICONS: Record<string, string> = {
  krx_stock_price: '🇰🇷',
  foreign_stock_price: '🌍',
  crypto_price: '₿',
  exchange_rate: '💱',
  market_index: '📈',
};

function App() {
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [selectedTable, setSelectedTable] = useState<TableInfo | null>(null);
  const [activeTab, setActiveTab] = useState<TabType>('info');
  const [schema, setSchema] = useState<TableSchema[]>([]);
  const [stats, setStats] = useState<TableStats | null>(null);
  const [data, setData] = useState<TableData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [initialLoading, setInitialLoading] = useState(true);

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
      setError('테이블 목록을 불러오는데 실패했습니다. 백엔드 서버를 확인하세요.');
      console.error(err);
    } finally {
      setInitialLoading(false);
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
    if (value === null || value === undefined) return '—';
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
  }

  if (initialLoading) {
    return (
      <div className="app">
        <div className="loading" style={{ height: '100vh' }}>
          <div className="spinner"></div>
          <p>데이터 카탈로그 로딩 중...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="app">
      <header className="header">
        <div className="header-content">
          <div className="logo">
            <div className="logo-icon">📊</div>
            <h1>DW Pipeline 데이터 카탈로그</h1>
          </div>
          <div className="header-badge">Iceberg + DuckDB</div>
        </div>
      </header>

      <main className="main-container">
        <aside className="sidebar">
          <div className="sidebar-card">
            <div className="sidebar-header">
              <h2>테이블 목록</h2>
            </div>
            <ul className="table-list">
              {tables.map((table) => (
                <li
                  key={table.id}
                  className={`table-item ${selectedTable?.id === table.id ? 'active' : ''}`}
                  onClick={() => setSelectedTable(table)}
                >
                  <h3>{TABLE_ICONS[table.id] || '📁'} {table.name}</h3>
                  <p>{table.id}</p>
                  <span className="source-badge">{table.source.split(' ')[0]}</span>
                </li>
              ))}
            </ul>
          </div>
        </aside>

        <section className="content">
          <div className="content-card">
            {selectedTable ? (
              <>
                <div className="tabs">
                  <button
                    className={`tab ${activeTab === 'info' ? 'active' : ''}`}
                    onClick={() => setActiveTab('info')}
                  >
                    📋 정보
                  </button>
                  <button
                    className={`tab ${activeTab === 'schema' ? 'active' : ''}`}
                    onClick={() => setActiveTab('schema')}
                  >
                    🏗️ 스키마
                  </button>
                  <button
                    className={`tab ${activeTab === 'data' ? 'active' : ''}`}
                    onClick={() => setActiveTab('data')}
                  >
                    📄 데이터
                  </button>
                  <button
                    className={`tab ${activeTab === 'query' ? 'active' : ''}`}
                    onClick={() => setActiveTab('query')}
                  >
                    ⚡ SQL 쿼리
                  </button>
                </div>

                <div className="tab-content">
                  {error && <div className="error">⚠️ {error}</div>}

                  {activeTab === 'info' && (
                    <div>
                      <div className="info-section">
                        <div className="info-header">
                          <div className="info-icon">
                            {TABLE_ICONS[selectedTable.id] || '📁'}
                          </div>
                          <div className="info-details">
                            <h3>{selectedTable.name}</h3>
                            <p>{selectedTable.description}</p>
                            <div className="info-badges">
                              <span className="badge badge-primary">
                                🔑 파티션: {selectedTable.partition}
                              </span>
                              <span className="badge badge-secondary">
                                📡 {selectedTable.source}
                              </span>
                            </div>
                          </div>
                        </div>
                      </div>

                      {loading ? (
                        <div className="loading">
                          <div className="spinner"></div>
                          <p>통계 로딩 중...</p>
                        </div>
                      ) : stats && (
                        <div className="stats-grid">
                          <div className="stat-card">
                            <div className="icon">📊</div>
                            <div className="value">{stats.record_count.toLocaleString()}</div>
                            <div className="label">총 레코드 수</div>
                          </div>
                          <div className="stat-card">
                            <div className="icon">📁</div>
                            <div className="value">{stats.partition_count.toLocaleString()}</div>
                            <div className="label">파티션 수</div>
                          </div>
                          <div className="stat-card">
                            <div className="icon">📅</div>
                            <div className="value">{stats.min_date || '—'}</div>
                            <div className="label">시작일</div>
                          </div>
                          <div className="stat-card">
                            <div className="icon">📅</div>
                            <div className="value">{stats.max_date || '—'}</div>
                            <div className="label">종료일</div>
                          </div>
                        </div>
                      )}
                    </div>
                  )}

                  {activeTab === 'schema' && (
                    <div>
                      {loading ? (
                        <div className="loading">
                          <div className="spinner"></div>
                          <p>스키마 로딩 중...</p>
                        </div>
                      ) : (
                        <div className="table-wrapper">
                          <table className="data-table">
                            <thead>
                              <tr>
                                <th>#</th>
                                <th>컬럼명</th>
                                <th>데이터 타입</th>
                              </tr>
                            </thead>
                            <tbody>
                              {schema.map((col, idx) => (
                                <tr key={idx}>
                                  <td style={{ color: 'var(--gray-500)' }}>{idx + 1}</td>
                                  <td><strong>{col.column_name}</strong></td>
                                  <td><span className="data-type">{col.data_type}</span></td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      )}
                    </div>
                  )}

                  {activeTab === 'data' && (
                    <div>
                      {loading ? (
                        <div className="loading">
                          <div className="spinner"></div>
                          <p>데이터 로딩 중...</p>
                        </div>
                      ) : data && data.rows.length > 0 ? (
                        <>
                          <div className="results-header">
                            <span className="count">✅ {data.row_count}개 행 로드됨</span>
                          </div>
                          <div className="table-wrapper">
                            <div className="scroll-container">
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
                          </div>
                        </>
                      ) : (
                        <div className="empty-state">
                          <div className="icon">📭</div>
                          <h3>데이터 없음</h3>
                          <p>테이블에 데이터가 없습니다.</p>
                        </div>
                      )}
                    </div>
                  )}

                  {activeTab === 'query' && (
                    <div className="sql-section">
                      <div className="sql-hint">
                        💡 SELECT 쿼리만 실행 가능합니다. iceberg_scan() 함수를 사용하세요.
                      </div>
                      <textarea
                        className="sql-editor"
                        value={sql}
                        onChange={(e) => setSql(e.target.value)}
                        placeholder={`SELECT * \nFROM iceberg_scan('s3://dw-pipeline-ch/warehouse/stock_data/${selectedTable.id}') \nLIMIT 10`}
                      />
                      <div>
                        <button
                          className="btn btn-primary"
                          onClick={handleQueryExecute}
                          disabled={queryLoading || !sql.trim()}
                        >
                          {queryLoading ? (
                            <>⏳ 실행 중...</>
                          ) : (
                            <>▶️ 쿼리 실행</>
                          )}
                        </button>
                      </div>

                      {queryError && (
                        <div className="error">⚠️ {queryError}</div>
                      )}

                      {queryResult && (
                        <div style={{ marginTop: '24px' }}>
                          <div className="results-header">
                            <span className="count">✅ 결과: {queryResult.row_count}개 행</span>
                          </div>
                          <div className="table-wrapper">
                            <div className="scroll-container">
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
                        </div>
                      )}
                    </div>
                  )}
                </div>
              </>
            ) : (
              <div className="empty-state">
                <div className="icon">👈</div>
                <h3>테이블을 선택하세요</h3>
                <p>왼쪽 목록에서 테이블을 선택하면 상세 정보를 확인할 수 있습니다.</p>
              </div>
            )}
          </div>
        </section>
      </main>
    </div>
  );
}

export default App;
