# Forestrat MCP Server - Prompts and Resources Guide

## Overview

The Forestrat MCP Server now supports **Prompts** and **Resources** in addition to tools, providing a more intelligent and contextual way to interact with your financial data lake.

## 🎯 Prompts

Prompts are predefined, intelligent query templates that combine multiple operations to provide comprehensive analysis. They're designed to answer complex questions with a single command.

### Available Prompts

#### 1. Daily Market Summary
**Purpose**: Get a comprehensive overview of market activity for a specific date and exchange.

**Usage Example**:
```
"Give me a daily market summary for LSE on 2025-01-15"
```

**What it includes**:
- Top 10 most active symbols by volume
- Top 10 least active symbols by volume
- Summary statistics and insights

**Parameters**:
- `date` (required): Trading date (YYYY-MM-DD)
- `exchange` (required): Exchange to analyze (LSE, CME, NYQ)
- `top_n` (optional): Number of symbols to include (default: 10)

#### 2. Cross-Exchange Symbol Analysis
**Purpose**: Compare how a specific symbol performed across different exchanges.

**Usage Example**:
```
"Analyze AAPL.O trading activity across all exchanges on 2025-01-15"
```

**What it includes**:
- Trading volume and count per exchange
- Price statistics (average, min, max)
- Cross-exchange comparison
- Data availability across exchanges

**Parameters**:
- `symbol` (required): Symbol to analyze (RIC format)
- `date` (required): Analysis date (YYYY-MM-DD)
- `exchanges` (optional): Comma-separated exchanges (default: LSE,CME,NYQ)

#### 3. Trading Anomaly Detection
**Purpose**: Identify unusual trading patterns and statistical outliers.

**Usage Example**:
```
"Detect trading anomalies for CME on 2025-01-15 with a 2.5 standard deviation threshold"
```

**What it includes**:
- Volume anomalies (symbols with unusual trading volumes)
- Statistical analysis (z-scores)
- Price volatility indicators
- Ranked list of anomalous symbols

**Parameters**:
- `date` (required): Date to analyze (YYYY-MM-DD)
- `exchange` (required): Exchange to analyze (LSE, CME, NYQ)
- `threshold` (optional): Standard deviation threshold (default: 3)

#### 4. Volume Trend Analysis
**Purpose**: Analyze volume trends and patterns over a date range.

**Usage Example**:
```
"Show me volume trends for NYQ from 2025-01-10 to 2025-01-15 for symbols MSFT.O,GOOGL.O"
```

**What it includes**:
- Daily volume breakdown by symbol
- Trend analysis over time
- Summary statistics
- Top performers each day

**Parameters**:
- `start_date` (required): Start date (YYYY-MM-DD)
- `end_date` (required): End date (YYYY-MM-DD)
- `exchange` (required): Exchange to analyze (LSE, CME, NYQ)
- `symbols` (optional): Comma-separated symbols to focus on

### How to Use Prompts

**In Claude Desktop**:
1. The prompts appear automatically in your conversation
2. Simply ask in natural language: *"Give me a daily market summary for LSE yesterday"*
3. Claude will map your request to the appropriate prompt and execute it

**Direct MCP Call**:
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "prompts/get",
  "params": {
    "name": "daily_market_summary",
    "arguments": {
      "date": "2025-01-15",
      "exchange": "LSE",
      "top_n": 15
    }
  }
}
```

## 📚 Resources

Resources provide dynamic, contextual information that Claude can reference during conversations to give better answers and validate inputs.

### Available Resources

#### 1. Schema Information
**URIs**:
- `forestrat://schemas/bronze_layer`
- `forestrat://schemas/silver_layer`
- `forestrat://schemas/gold_layer`

**What they provide**:
- Current table schemas and column definitions
- Data types and nullable information
- Layer-specific descriptions

**Usage**: Claude automatically references these when helping with SQL queries or data structure questions.

#### 2. Trading Calendars
**URIs**:
- `forestrat://calendars/lse_trading_days`
- `forestrat://calendars/cme_trading_days`
- `forestrat://calendars/nyq_trading_days`

**What they provide**:
- Available trading dates for each exchange
- Trade counts and symbol counts per day
- Date ranges and data availability

**Usage**: Claude uses these to validate dates and suggest available data ranges.

#### 3. Symbol Directories
**URIs**:
- `forestrat://mappings/symbols/LSE`
- `forestrat://mappings/symbols/CME`
- `forestrat://mappings/symbols/NYQ`

**What they provide**:
- Complete symbol lists per exchange
- Trading statistics per symbol
- Historical data availability
- Top 1000 symbols by volume

**Usage**: Claude references these for symbol validation and suggestions.

#### 4. Data Quality Reports
**URI**: `forestrat://reports/data_quality`

**What it provides**:
- Data completeness metrics
- Null value percentages
- Record counts per exchange
- Quality indicators

**Usage**: Claude uses this to explain data gaps or quality issues.

#### 5. Database Overview
**URI**: `forestrat://stats/database_overview`

**What it provides**:
- High-level database statistics
- Schema and table counts
- Overall data summary

**Usage**: Claude references this for general database information.

## 🚀 Real-World Usage Examples

### Example 1: Market Analysis Workflow
```
User: "I need to analyze unusual trading activity for CME yesterday"

Claude Response:
1. References trading calendar to confirm data availability
2. Executes anomaly detection prompt
3. Cross-references with symbol directory for context
4. Provides detailed anomaly report with statistical analysis
```

### Example 2: Symbol Research
```
User: "Tell me about AAPL.O trading patterns last week"

Claude Response:
1. Checks symbol directory to confirm AAPL.O exists
2. Uses volume trend analysis for the week
3. Cross-exchange analysis to compare performance
4. References schema to explain data fields
```

### Example 3: Data Quality Investigation
```
User: "Why am I seeing missing data for some symbols?"

Claude Response:
1. References data quality report
2. Shows null percentages and completeness metrics
3. Uses trading calendar to identify data gaps
4. Explains potential causes based on exchange patterns
```

## 🔧 Technical Implementation

### Prompt Execution Flow
1. **Input Validation**: Required parameters checked
2. **Multi-Tool Execution**: Combines multiple tool calls
3. **Data Aggregation**: Merges results from different sources
4. **Formatted Output**: Returns structured markdown report

### Resource Reading Flow
1. **URI Parsing**: Determines resource type and parameters
2. **Dynamic Query**: Builds SQL based on resource requirements
3. **Real-time Data**: Always returns current information
4. **JSON Response**: Structured data for Claude to reference

### Error Handling
- **Missing Parameters**: Clear error messages with required fields
- **Invalid Exchanges**: Lists available options
- **Data Unavailability**: Graceful degradation with explanations
- **SQL Errors**: Helpful debugging information

## 🎯 Best Practices

### Using Prompts Effectively
1. **Start with prompts** for complex analysis
2. **Use specific dates** for better performance
3. **Combine prompts** for comprehensive insights
4. **Leverage natural language** - no need for exact syntax

### Resource Optimization
1. **Resources are cached** by Claude for efficiency
2. **Schemas update dynamically** when database changes
3. **Trading calendars reflect real data** availability
4. **Quality reports are real-time**

### Performance Tips
1. **Prompts are optimized** for large datasets
2. **Use date ranges judiciously** for trend analysis
3. **Symbol filtering** improves query performance
4. **Resources provide quick context** without heavy queries

## 🔮 Advanced Usage

### Combining Prompts and Resources
```
User: "Find unusual trading patterns in LSE data and explain what might cause them"

Claude Workflow:
1. Uses trading calendar resource to identify recent active dates
2. Executes anomaly detection prompt
3. References symbol directory for context about flagged symbols
4. Uses data quality report to check for data issues
5. Provides comprehensive analysis with multiple perspectives
```

### Custom Analysis Workflows
The prompts and resources can be combined to create powerful analytical workflows:

1. **Market Health Check**: Daily summary + anomaly detection + data quality
2. **Symbol Deep Dive**: Cross-exchange analysis + volume trends + symbol directory
3. **Data Validation**: Trading calendar + data quality + schema verification

## 🛠️ Development Notes

### Adding New Prompts
1. Define in `handle_list_prompts()`
2. Add execution method `_execute_[prompt_name]()`
3. Update request handler routing
4. Test with various parameter combinations

### Adding New Resources
1. Define URI pattern in `handle_list_resources()`
2. Add reading method `_read_[resource_type]_resource()`
3. Update URI routing logic
4. Ensure real-time data freshness

---

This enhanced MCP server now provides a much richer, more intelligent interface to your financial data lake, combining the precision of tools with the intelligence of prompts and the context of resources. 