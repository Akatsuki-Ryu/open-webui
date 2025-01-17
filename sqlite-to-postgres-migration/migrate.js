// @ts-nocheck
// Credit goes to Anthropic Claude Sonnet 3.5 for helping write this script!
const sqlite3 = require('sqlite3').verbose();
const { Client } = require('pg');
const fs = require('fs').promises;

// Configuration
const SQLITE_DB_PATH = 'webui.db';
const BATCH_SIZE = 500;
const MAX_RETRIES = 3;

const PG_CONFIG = {
  host: 'localhost',
  port: 5433,
  database: 'appdb',
  user: 'appuser',
  password: 'apppassword'
};

// Helper function to convert SQLite types to PostgreSQL types
function sqliteToPgType(sqliteType) {
  switch (sqliteType.toUpperCase()) {
    case 'INTEGER': return 'INTEGER';
    case 'REAL': return 'DOUBLE PRECISION';
    case 'TEXT': return 'TEXT';
    case 'BLOB': return 'BYTEA';
    default: return 'TEXT';
  }
}

// Helper function to handle reserved keywords
function getSafeIdentifier(identifier) {
  const reservedKeywords = ['user', 'group', 'order', 'table', 'select', 'where', 'from', 'index', 'constraint'];
  return reservedKeywords.includes(identifier.toLowerCase()) ? `"${identifier}"` : identifier;
}

// Check SQLite database integrity
async function checkSqliteIntegrity(sqliteDb) {
  console.log("Running SQLite database integrity check...");
  
  try {
    // Run integrity checks
    const integrityCheck = await new Promise((resolve, reject) => {
      sqliteDb.get("PRAGMA integrity_check;", (err, result) => {
        if (err) reject(err);
        else resolve(result);
      });
    });

    const quickCheck = await new Promise((resolve, reject) => {
      sqliteDb.get("PRAGMA quick_check;", (err, result) => {
        if (err) reject(err);
        else resolve(result);
      });
    });

    const fkCheck = await new Promise((resolve, reject) => {
      sqliteDb.all("PRAGMA foreign_key_check;", (err, result) => {
        if (err) reject(err);
        else resolve(result);
      });
    });

    if (integrityCheck.integrity_check !== 'ok') {
      console.log("❌ Database integrity check failed!");
      console.log("Integrity check results:", integrityCheck);
      return false;
    }

    if (quickCheck.quick_check !== 'ok') {
      console.log("❌ Quick check failed!");
      console.log("Quick check results:", quickCheck);
      return false;
    }

    if (fkCheck && fkCheck.length > 0) {
      console.log("❌ Foreign key check failed!");
      console.log("Foreign key issues:", fkCheck);
      return false;
    }

    // Test basic query
    await new Promise((resolve, reject) => {
      sqliteDb.get("SELECT COUNT(*) FROM sqlite_master;", (err, result) => {
        if (err) reject(err);
        else resolve(result);
      });
    });

    console.log("✅ SQLite database integrity check passed");
    return true;
  } catch (error) {
    console.log("❌ Error during integrity check:", error);
    return false;
  }
}

async function migrate() {
  // Connect to SQLite database
  const sqliteDb = new sqlite3.Database(SQLITE_DB_PATH);

  // Set SQLite optimizations
  await new Promise((resolve) => {
    sqliteDb.run('PRAGMA journal_mode=WAL', () => resolve());
  });
  await new Promise((resolve) => {
    sqliteDb.run('PRAGMA synchronous=NORMAL', () => resolve());
  });

  // Connect to PostgreSQL database
  const pgClient = new Client(PG_CONFIG);
  await pgClient.connect();

  try {
    // Check database integrity first
    const isIntegrityOk = await checkSqliteIntegrity(sqliteDb);
    if (!isIntegrityOk) {
      console.log("Aborting migration due to database integrity issues");
      process.exit(1);
    }

    // Get list of tables from SQLite
    const tables = await new Promise((resolve, reject) => {
      sqliteDb.all("SELECT name FROM sqlite_master WHERE type='table'", (err, rows) => {
        if (err) reject(err);
        else resolve(rows);
      });
    });

    for (const table of tables) {
      const tableName = table.name;

      // Skip system tables
      if (tableName === "migratehistory" || tableName === "alembic_version") {
        console.log(`Skipping table: ${tableName}`);
        continue;
      }

      const safeTableName = getSafeIdentifier(tableName);
      console.log(`\nProcessing table: ${tableName}`);

      // Truncate existing table
      try {
        console.log(`Truncating table: ${tableName}`);
        await pgClient.query(`TRUNCATE TABLE ${safeTableName} CASCADE`);
      } catch (error) {
        console.log(`Note: Table ${tableName} does not exist yet or could not be truncated:`, error);
      }

      // Get table schema from PostgreSQL
      const pgSchema = await pgClient.query(
        `SELECT column_name, data_type
         FROM information_schema.columns
         WHERE table_name = $1`,
        [safeTableName]
      );

      const pgColumnTypes = {};
      pgSchema.rows.forEach((col) => {
        pgColumnTypes[col.column_name] = col.data_type;
      });

      // Get table schema from SQLite with retries
      let schema;
      let retryCount = 0;
      while (retryCount < MAX_RETRIES) {
        try {
          schema = await new Promise((resolve, reject) => {
            sqliteDb.all(`PRAGMA table_info(\`${tableName}\`)`, (err, rows) => {
              if (err) reject(err);
              else resolve(rows);
            });
          });
          break;
        } catch (error) {
          retryCount++;
          console.log(`Retry ${retryCount}/${MAX_RETRIES} getting schema for ${tableName}:`, error);
          if (retryCount === MAX_RETRIES) throw error;
        }
      }

      // Create table if it doesn't exist in PostgreSQL
      if (Object.keys(pgColumnTypes).length === 0) {
        const columns = schema.map((col) => 
          `${getSafeIdentifier(col.name)} ${sqliteToPgType(col.type)}`
        ).join(', ');
        await pgClient.query(`CREATE TABLE IF NOT EXISTS ${safeTableName} (${columns})`);
      }

      // Get total count for progress tracking
      const countResult = await new Promise((resolve, reject) => {
        sqliteDb.get(`SELECT COUNT(*) as count FROM \`${tableName}\``, (err, result) => {
          if (err) reject(err);
          else resolve(result);
        });
      });
      const totalRows = countResult.count;
      let processedRows = 0;
      const failedRows = [];

      // Process data in batches
      while (true) {
        try {
          const rows = await new Promise((resolve, reject) => {
            sqliteDb.all(
              `SELECT * FROM \`${tableName}\` LIMIT ${BATCH_SIZE} OFFSET ${processedRows}`,
              (err, rows) => {
                if (err) reject(err);
                else resolve(rows);
              }
            );
          });

          if (rows.length === 0) break;

          // Process each row in the batch
          for (const row of rows) {
            try {
              const columns = Object.keys(row).map(getSafeIdentifier);
              const values = Object.entries(row).map(([key, value]) => {
                const columnType = pgColumnTypes[key];

                if (value === null) return 'NULL';
                
                if (columnType === 'boolean') {
                  return value === 1 ? 'true' : 'false';
                }

                if (Buffer.isBuffer(value)) {
                  // Handle BLOB/Buffer data
                  try {
                    return `'${value.toString('utf8').replace(/'/g, "''")}'`;
                  } catch {
                    return `'${value.toString('latin1').replace(/'/g, "''")}'`;
                  }
                }

                if (typeof value === 'string') {
                  const cleanedValue = value
                    .replace(/\x00/g, '') // Remove null bytes
                    .replace(/'/g, "''"); // Escape single quotes
                  return `'${cleanedValue}'`;
                }

                return value;
              });

              const query = `
                INSERT INTO ${safeTableName} 
                (${columns.join(', ')}) 
                VALUES (${values.join(', ')})
              `;
              await pgClient.query(query);
            } catch (error) {
              console.log(`Error processing row in ${tableName}:`, error);
              failedRows.push([tableName, processedRows + failedRows.length, error.message]);
              continue;
            }
          }

          processedRows += rows.length;
          console.log(`Processed ${processedRows}/${totalRows} rows from ${tableName}`);
          await pgClient.query('COMMIT');

        } catch (error) {
          console.log(`SQLite error during batch processing:`, error);
          console.log("Attempting to continue with next batch...");
          processedRows += BATCH_SIZE;
          continue;
        }
      }

      if (failedRows.length > 0) {
        console.log(`\nFailed rows for ${tableName}:`);
        for (const [table, rowNum, error] of failedRows) {
          console.log(`Row ${rowNum}: ${error}`);
        }
      }

      console.log(`Completed migrating ${processedRows} rows from ${tableName}`);
      console.log(`Failed to migrate ${failedRows.length} rows from ${tableName}`);
    }

    console.log("\nMigration completed!");
    const totalFailedRows = tables.reduce((sum, table) => sum + (table.failedRows?.length || 0), 0);
    if (totalFailedRows > 0) {
      console.log(`Total failed rows: ${totalFailedRows}`);
    }

  } catch (error) {
    console.error("Critical error during migration:", error);
    console.error("Stack trace:", error.stack);
    await pgClient.query('ROLLBACK');
  } finally {
    // Close database connections
    sqliteDb.close();
    await pgClient.end();
  }
}

migrate().catch(console.error);