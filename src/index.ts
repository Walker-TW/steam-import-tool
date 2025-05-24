import * as fs from 'fs';
import * as path from 'path';
import csv from 'csv-parser';
import { Database } from 'sqlite3';
import { createReadStream } from 'fs';

// Interface for Steam Game data structure
interface SteamGame {
  app_id: number;
  name: string;
  release_date: string;
  estimated_owners: string;
  peak_ccu: number;
  required_age: number;
  price: number;
  discount_dlc_count: number;
  about_the_game: string;
  supported_languages: string;
  full_audio_languages: string;
  reviews: string;
  header_image: string;
  website: string;
  support_url: string;
  support_email: string;
  windows: string;
  mac: string;
  linux: string;
  metacritic_score: string;
  metacritic_url: string;
  user_score: string;
  positive: number;
  negative: number;
  score_rank: number;
  achievements: number | null;
  recommendations: number;
  notes: string;
  average_playtime_forever: string;
  average_playtime_two_weeks: number;
  median_playtime_forever: number;
  median_playtime_two_weeks: number;
  developers: string;
  publishers: string;
  categories: string;
  genres: string;
  tags: string;
  screenshots: string;
  movies: string;
}

// Configuration interface
interface ImportConfig {
  csvFilePath: string;
  dbPath: string;
  chunkSize: number;
  tableName: string;
}

class SteamCSVImporter {
  private db: Database;
  private config: ImportConfig;
  private processedRows: number = 0;

  constructor(config: ImportConfig) {
    this.config = config;
    this.db = new Database(config.dbPath);
    this.db.serialize(); // Ensure operations run in sequence
  }

  // Create the SQLite table with proper schema
  private async createTable(): Promise<void> {
    return new Promise((resolve, reject) => {
      const createTableSQL = `
        CREATE TABLE IF NOT EXISTS ${this.config.tableName} (
          app_id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          release_date TEXT,
          estimated_owners TEXT,
          peak_ccu INTEGER,
          required_age INTEGER,
          price REAL,
          discount_dlc_count INTEGER,
          about_the_game TEXT,
          supported_languages TEXT,
          full_audio_languages TEXT,
          reviews TEXT,
          header_image TEXT,
          website TEXT,
          support_url TEXT,
          support_email TEXT,
          windows TEXT,
          mac TEXT,
          linux TEXT,
          metacritic_score TEXT,
          metacritic_url TEXT,
          user_score TEXT,
          positive INTEGER,
          negative INTEGER,
          score_rank INTEGER,
          achievements INTEGER,
          recommendations INTEGER,
          notes TEXT,
          average_playtime_forever TEXT,
          average_playtime_two_weeks INTEGER,
          median_playtime_forever INTEGER,
          median_playtime_two_weeks INTEGER,
          developers TEXT,
          publishers TEXT,
          categories TEXT,
          genres TEXT,
          tags TEXT,
          screenshots TEXT,
          movies TEXT,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
      `;

      this.db.run(createTableSQL, (err) => {
        if (err) {
          reject(err);
        } else {
          console.log(`✅ Table '${this.config.tableName}' created successfully`);
          resolve();
        }
      });
    });
  }

  // Clean and normalize column names
  private cleanColumnName(columnName: string): string {
    return columnName
      .toLowerCase()
      .replace(/\s+/g, '_')
      .replace(/-/g, '_')
      .replace(/[^\w]/g, '');
  }

  // Clean and convert data types
  private cleanRowData(row: any): Partial<SteamGame> {
    const cleanedRow: any = {};

    // Map original column names to cleaned versions
    Object.keys(row).forEach(key => {
      const cleanKey = this.cleanColumnName(key);
      let value = row[key];

      // Handle empty strings and convert to null
      if (value === '' || value === undefined) {
        value = null;
      }

      // Type conversions based on column
      switch (cleanKey) {
        case 'app_id':
        case 'appid':
          cleanedRow.app_id = value ? parseInt(value) : null;
          break;
        case 'peak_ccu':
        case 'required_age':
        case 'discount_dlc_count':
        case 'positive':
        case 'negative':
        case 'score_rank':
        case 'recommendations':
        case 'average_playtime_two_weeks':
        case 'median_playtime_forever':
        case 'median_playtime_two_weeks':
          cleanedRow[cleanKey] = value ? parseInt(value) : 0;
          break;
        case 'price':
          cleanedRow[cleanKey] = value ? parseFloat(value) : 0.0;
          break;
        case 'achievements':
          cleanedRow[cleanKey] = value && value !== '0' ? parseInt(value) : null;
          break;
        case 'windows':
        case 'mac':
        case 'linux':
          // Convert boolean-like strings
          if (value === 'True' || value === 'true' || value === true) {
            cleanedRow[cleanKey] = 'True';
          } else if (value === 'False' || value === 'false' || value === false) {
            cleanedRow[cleanKey] = 'False';
          } else {
            cleanedRow[cleanKey] = value;
          }
          break;
        default:
          cleanedRow[cleanKey] = value;
      }
    });

    return cleanedRow;
  }

  // Process CSV in chunks
  async importCSV(): Promise<void> {
    await this.createTable();

    return new Promise((resolve, reject) => {
      let currentChunk: any[] = [];
      let insertCount = 0;

      // Prepare insert statement
      const insertSQL = `
        INSERT OR IGNORE INTO ${this.config.tableName} (
          app_id, name, release_date, estimated_owners, peak_ccu, required_age,
          price, discount_dlc_count, about_the_game, supported_languages,
          full_audio_languages, reviews, header_image, website, support_url,
          support_email, windows, mac, linux, metacritic_score, metacritic_url,
          user_score, positive, negative, score_rank, achievements, recommendations,
          notes, average_playtime_forever, average_playtime_two_weeks,
          median_playtime_forever, median_playtime_two_weeks, developers,
          publishers, categories, genres, tags, screenshots, movies
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `;

      const insertStatement = this.db.prepare(insertSQL);

      // Create read stream and process CSV
      createReadStream(this.config.csvFilePath)
        .pipe(csv())
        .on('data', (row) => {
          const cleanedRow = this.cleanRowData(row);
          currentChunk.push(cleanedRow);

          // Process chunk when it reaches the specified size
          if (currentChunk.length >= this.config.chunkSize) {
            this.insertChunk(currentChunk, insertStatement);
            currentChunk = [];
          }
        })
        .on('end', () => {
          // Process remaining rows
          if (currentChunk.length > 0) {
            this.insertChunk(currentChunk, insertStatement);
          }
          
          insertStatement.finalize();
          this.createIndexes().then(() => {
            console.log(`🎉 Import completed! Total rows processed: ${this.processedRows}`);
            resolve();
          }).catch(reject);
        })
        .on('error', (error) => {
          console.error('❌ Error reading CSV:', error);
          reject(error);
        });
    });
  }

  // Insert a chunk of data using transactions for performance
  private insertChunk(chunk: any[], insertStatement: any): void {
    this.db.serialize(() => {
      this.db.run('BEGIN TRANSACTION');
      
      chunk.forEach((row) => {
        const values = [
          row.app_id, row.name, row.release_date, row.estimated_owners,
          row.peak_ccu, row.required_age, row.price, row.discount_dlc_count,
          row.about_the_game, row.supported_languages, row.full_audio_languages,
          row.reviews, row.header_image, row.website, row.support_url,
          row.support_email, row.windows, row.mac, row.linux,
          row.metacritic_score, row.metacritic_url, row.user_score,
          row.positive, row.negative, row.score_rank, row.achievements,
          row.recommendations, row.notes, row.average_playtime_forever,
          row.average_playtime_two_weeks, row.median_playtime_forever,
          row.median_playtime_two_weeks, row.developers, row.publishers,
          row.categories, row.genres, row.tags, row.screenshots, row.movies
        ];

        insertStatement.run(values, (err: any) => {
          if (err) {
            console.warn(`⚠️ Error inserting row ${row.app_id}:`, err.message);
          } else {
            this.processedRows++;
          }
        });
      });
      
      this.db.run('COMMIT');
    });

    console.log(`📦 Processed chunk: ${chunk.length} rows (Total: ${this.processedRows})`);
  }

  // Create performance indexes
  private async createIndexes(): Promise<void> {
    const indexes = [
      `CREATE INDEX IF NOT EXISTS idx_steam_app_id ON ${this.config.tableName}(app_id)`,
      `CREATE INDEX IF NOT EXISTS idx_steam_name ON ${this.config.tableName}(name)`,
      `CREATE INDEX IF NOT EXISTS idx_steam_price ON ${this.config.tableName}(price)`,
      `CREATE INDEX IF NOT EXISTS idx_steam_positive ON ${this.config.tableName}(positive)`,
      `CREATE INDEX IF NOT EXISTS idx_steam_release_date ON ${this.config.tableName}(release_date)`,
      `CREATE INDEX IF NOT EXISTS idx_steam_price_positive ON ${this.config.tableName}(price, positive)`
    ];

    return new Promise((resolve, reject) => {
      let completed = 0;
      
      indexes.forEach((indexSQL, i) => {
        this.db.run(indexSQL, (err) => {
          if (err) {
            console.warn(`⚠️ Error creating index ${i + 1}:`, err.message);
          } else {
            console.log(`📊 Created index ${i + 1}/${indexes.length}`);
          }
          
          completed++;
          if (completed === indexes.length) {
            resolve();
          }
        });
      });
    });
  }

  // Get statistics about the imported data
  async getStats(): Promise<any> {
    return new Promise((resolve, reject) => {
      const stats: any = {};
      
      // Get total count
      this.db.get(`SELECT COUNT(*) as count FROM ${this.config.tableName}`, (err, row: any) => {
        if (err) {
          reject(err);
          return;
        }
        stats.totalGames = row;
        
        // Get average price
        this.db.get(`SELECT AVG(price) as avg_price FROM ${this.config.tableName} WHERE price > 0`, (err, row: any) => {
          if (err) {
            reject(err);
            return;
          }
          stats.avgPrice = row;
          
          // Get price range
          this.db.get(`SELECT MIN(price) as min_price, MAX(price) as max_price FROM ${this.config.tableName} WHERE price > 0`, (err, row: any) => {
            if (err) {
              reject(err);
              return;
            }
            stats.priceRange = row;
            resolve(stats);
          });
        });
      });
    });
  }

  // Close database connection
  close(): void {
    this.db.close((err) => {
      if (err) {
        console.error('Error closing database:', err.message);
      } else {
        console.log('🔒 Database connection closed');
      }
    });
  }
}

// Usage example and main execution
async function main() {
  const config: ImportConfig = {
    csvFilePath: './steam_games.csv', // Update this path
    dbPath: './steam_games.db',
    chunkSize: 1000,
    tableName: 'steam_games'
  };

  console.log('🚀 Starting Steam Games CSV import...');
  console.log(`📁 CSV File: ${config.csvFilePath}`);
  console.log(`💾 Database: ${config.dbPath}`);
  console.log(`📦 Chunk Size: ${config.chunkSize}`);
  console.log('─'.repeat(50));

  const importer = new SteamCSVImporter(config);

  try {
    await importer.importCSV();
    
    // Display statistics
    console.log('\n📈 Import Statistics:');
    const stats = await importer.getStats();
    console.log(JSON.stringify(stats, null, 2));
    
  } catch (error) {
    console.error('💥 Import failed:', error);
  } finally {
    importer.close();
  }
}

// Export for use as module
export { SteamCSVImporter, ImportConfig, SteamGame };

// Run if this file is executed directly
if (require.main === module) {
  main().catch(console.error);
}