# 🔄 No-Code ETL Studio

A powerful, user-friendly ETL (Extract, Transform, Load) application built with Streamlit that allows you to upload raw data, clean it using Python/SQL logic, and export high-quality datasets without writing code.

## ✨ Features

### 🏗️ Architecture
- **Session State Management**: Maintains transaction logs and data state across operations
- **Caching System**: Optimized performance with `@st.cache_data` for data loading
- **Multi-DataFrame Support**: Work with multiple datasets simultaneously
- **Transaction Logging**: Complete audit trail of all data transformations

### 📥 Phase 1: Source (Ingestion)
- **File Upload**: CSV, Excel, JSON, Parquet support
- **Database Connection**: SQLAlchemy-compatible database connections
- **Data Preview**: Immediate preview of uploaded data
- **Metadata Display**: File size, dimensions, and structure

### 🔍 Phase 2: Profile (Quality Check)
- **Data Health Score**: Overall quality metric (0-100%)
- **Missing Values Analysis**: Visual heatmap and statistics
- **Duplicate Detection**: Row-level duplicate identification
- **Data Type Analysis**: Column-wise type distribution
- **Schema Information**: Complete column documentation

### 🧮 Phase 3: SQL Workbench (Killer Feature)
- **DuckDB Integration**: SQL queries directly on DataFrames
- **Syntax Highlighting**: Full SQL editor with autocomplete
- **Multi-Table Support**: Query across all loaded DataFrames
- **Query History**: Save and reuse previous queries
- **Result Export**: Save query results as new DataFrames
- **SQL Examples**: Pre-built templates for common operations

### 🔧 Phase 4: Transform
- **No-Code Operations**: 
  - Drop duplicates
  - Fill missing values (forward fill, backward fill, zero, mean, median)
  - Trim whitespace
- **Column Renaming**: Bulk column name mapping
- **Type Casting**: Convert between data types
- **Real-time Preview**: See changes before applying

### 📤 Phase 5: Sink (Export)
- **Multiple Formats**: CSV, Parquet, JSON, Excel
- **Compression Options**: gzip, zip, bz2, xz support
- **Quality Metrics**: Final data quality assessment
- **Transaction Log Export**: Complete audit trail

## 📸 Screenshots

### SQL Workbench - Query Your Data
![SQL Workbench Interface](https://github.com/DukeAche/Etl_studio/blob/main/Screenshot%202025-11-24%209.19.07%20PM.png)

### Data Ingestion Phase
![Data Ingestion Interface](/home/dukeray66/.gemini/antigravity/brain/f856fd23-c9b3-4c29-ba8e-e60a449ee3c6/data_ingestion.png)

### Data Transformation & Cleaning
![Data Transformation Interface](/home/dukeray66/.gemini/antigravity/brain/f856fd23-c9b3-4c29-ba8e-e60a449ee3c6/data_transformation.png)

### Data Export
![Data Export Interface](/home/dukeray66/.gemini/antigravity/brain/f856fd23-c9b3-4c29-ba8e-e60a449ee3c6/data_export.png)

## 🚀 Quick Start

### Installation

1. **Clone or download the project files**
2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the application**:
   ```bash
   streamlit run app.py
   ```

4. **Open your browser** to the provided URL (typically `http://localhost:8501`)

### First Steps

1. **Upload Data**: Go to "Source" tab and upload a CSV, Excel, JSON, or Parquet file
2. **Profile Data**: Check the "Profile" tab to understand data quality
3. **Query with SQL**: Use the "SQL Workbench" to run SQL queries on your data
4. **Clean Data**: Apply transformations in the "Transform" tab
5. **Export Results**: Download your cleaned dataset from the "Export" tab

## 💡 Usage Examples

### SQL Workbench Examples

```sql
-- Basic data exploration
SELECT * FROM df LIMIT 10;

-- Filter and aggregate
SELECT category, COUNT(*) as count, AVG(sales) as avg_sales
FROM df 
WHERE date >= '2023-01-01'
GROUP BY category
ORDER BY avg_sales DESC;

-- Data cleaning query
SELECT 
    TRIM(customer_name) as clean_name,
    COALESCE(sales, 0) as clean_sales,
    CAST(order_date as DATE) as clean_date
FROM df
WHERE customer_name IS NOT NULL;
```

### Data Transformation Pipeline

1. **Upload** raw CSV file with customer data
2. **Profile** to find 15% missing values and 2% duplicates
3. **SQL Query** to filter active customers and calculate metrics
4. **Transform** to fill missing values with median, drop duplicates
5. **Export** as Parquet for optimal performance

## 🔧 Technical Details

### Dependencies
- **Streamlit**: Web framework and UI components
- **Pandas**: Data manipulation and analysis
- **DuckDB**: In-memory SQL database for DataFrame queries
- **Plotly**: Interactive visualizations
- **SQLAlchemy**: Database connection management
- **Streamlit-ACE**: Advanced code editor for SQL queries

### Session State Architecture
```python
# Core session state variables
- transaction_log: List of all operations performed
- current_df: Name of currently active DataFrame
- original_df: Reference to original uploaded data
- dataframes: Dictionary of all loaded DataFrames
- query_history: History of SQL queries executed
```

### Performance Features
- **Caching**: Data loading cached with `@st.cache_data`
- **Lazy Loading**: Efficient memory management
- **Compression**: Optimized data export formats
- **Progress Indicators**: Real-time operation feedback

## 📊 Data Flow

```
Raw Data (Bronze) → Profile → SQL Queries → Transform → Clean Data (Gold)
     ↓                    ↓         ↓          ↓              ↓
   Upload            Analyze    Filter     Clean        Export
   Ingest           Quality     Join      Type Cast    Download
   Preview          Metrics    Aggregate  Fill Null    Audit Log
```

## 🎯 Best Practices

1. **Start Small**: Test with sample data before processing large datasets
2. **Check Quality**: Always profile data before transformation
3. **Use SQL**: Leverage SQL Workbench for complex filtering and joins
4. **Save Progress**: Export intermediate results for complex pipelines
5. **Document**: Use transaction logs for reproducibility

## 🔍 Troubleshooting

### Common Issues

1. **Memory Errors**: Large datasets may exceed memory limits
   - Solution: Process data in chunks or use sampling

2. **SQL Syntax Errors**: Incorrect table names or syntax
   - Solution: Check available tables in the schema explorer

3. **Type Conversion Failures**: Incompatible data types
   - Solution: Clean data before type conversion

4. **Database Connection Issues**: Invalid connection strings
   - Solution: Verify SQLAlchemy compatibility

### Performance Tips

- Use Parquet format for large datasets
- Apply filters early in the pipeline
- Cache expensive operations
- Monitor memory usage in real-time

## 🏢 Enterprise Features

- **Audit Trail**: Complete transaction logging
- **Data Lineage**: Track transformations from source to target
- **Quality Metrics**: Built-in data quality assessment
- **Multi-Format Support**: Industry-standard data formats
- **Database Integration**: Enterprise database connectivity

## 🤝 Contributing

This ETL Studio is designed to be extensible. Key areas for enhancement:

- Additional data sources (APIs, cloud storage)
- Advanced transformations (feature engineering)
- Visualization enhancements
- Performance optimizations
- Additional export formats

## 📄 License

This project is open source and available under the MIT License.

---

**Built with ❤️ using Streamlit, DuckDB, and Pandas**
