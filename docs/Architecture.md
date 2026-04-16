# Velora Analytics — Architecture

## Data Flow
Source CSVs → Bronze Lakehouse → Silver Lakehouse → Gold Lakehouse → Power BI

## Layer Responsibilities

### Bronze
- Raw data landed as-is
- No transformations
- Append only
- Load log maintained

### Silver
- Null handling
- Deduplication
- Business flags added
- Category translation

### Gold
- Star schema
- Fact and dimension tables
- Ready for Power BI