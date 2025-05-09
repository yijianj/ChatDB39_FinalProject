import React, { useState } from 'react';
import {
  Container,
  TextField,
  Button,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Box,
  Typography,
  Paper,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Tabs,
  Tab
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import axios from 'axios';

function DataTable({ data, title }) {
  if (!data || data.length === 0) {
    return (
      <Typography variant="subtitle1" gutterBottom>
        No data found for {title}
      </Typography>
    );
  }

  // Collect all unique keys for table headers
  const allKeys = new Set();
  data.forEach((item) => {
    Object.keys(item).forEach((key) => allKeys.add(key));
  });
  const columns = Array.from(allKeys);

  // Format cell value to handle different data types
  const formatCellValue = (value) => {
    if (value === null || value === undefined) {
      return '';
    }
    
    // Handle arrays
    if (Array.isArray(value)) {
      return value.map(item => 
        typeof item === 'object' && item !== null 
          ? JSON.stringify(item) 
          : String(item)
      ).join(', ');
    }
    
    // Handle objects
    if (typeof value === 'object' && value !== null) {
      return JSON.stringify(value);
    }
    
    // Handle primitive values
    return String(value);
  };

  return (
    <Box sx={{ mb: 2 }}>
      <Typography variant="subtitle1" gutterBottom>
        {title}
      </Typography>
      <Box sx={{ overflowX: 'auto', border: '1px solid #ddd' }}>
        <table style={{ borderCollapse: 'collapse', width: '100%' }}>
          <thead>
            <tr>
              {columns.map((col) => (
                <th
                  key={col}
                  style={{
                    padding: '8px',
                    background: '#f0f0f0',
                    borderBottom: '1px solid #ccc',
                    textAlign: 'left'
                  }}
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.map((row, rowIndex) => (
              <tr key={rowIndex}>
                {columns.map((col) => (
                  <td
                    key={col}
                    style={{
                      padding: '8px',
                      borderBottom: '1px solid #eee',
                      verticalAlign: 'top'
                    }}
                  >
                    {formatCellValue(row[col])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </Box>
    </Box>
  );
}

// Component to display schema information
function SchemaDisplay({ data, title }) {
  if (!data || data.length === 0) {
    return (
      <Typography variant="subtitle1" gutterBottom>
        No schema information available for {title}
      </Typography>
    );
  }

  return (
    <Box sx={{ mb: 2 }}>
      <Typography variant="subtitle1" gutterBottom>
        {title}
      </Typography>
      <Box sx={{ overflowX: 'auto', border: '1px solid #ddd' }}>
        <table style={{ borderCollapse: 'collapse', width: '100%' }}>
          <thead>
            <tr>
              {Object.keys(data[0]).map((col) => (
                <th
                  key={col}
                  style={{
                    padding: '8px',
                    background: '#f0f0f0',
                    borderBottom: '1px solid #ccc',
                    textAlign: 'left'
                  }}
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.map((row, rowIndex) => (
              <tr key={rowIndex}>
                {Object.entries(row).map(([key, value]) => (
                  <td
                    key={key}
                    style={{
                      padding: '8px',
                      borderBottom: '1px solid #eee',
                      verticalAlign: 'top'
                    }}
                  >
                    {value !== null && value !== undefined ? String(value) : ''}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </Box>
    </Box>
  );
}

// Component to display Firebase/complex schema
function StructureDisplay({ data, title }) {
  if (!data) {
    return (
      <Typography variant="subtitle1" gutterBottom>
        No structure information available for {title}
      </Typography>
    );
  }

  return (
    <Box sx={{ mb: 2 }}>
      <Typography variant="subtitle1" gutterBottom>
        {title}
      </Typography>
      <Paper elevation={0} sx={{ p: 2, backgroundColor: '#f5f5f5' }}>
        <pre style={{ margin: 0, overflow: 'auto' }}>
          {JSON.stringify(data, null, 2)}
        </pre>
      </Paper>
    </Box>
  );
}

// Component to display a list of tables/collections/nodes
function ListDisplay({ data, title }) {
  if (!data || data.length === 0) {
    return (
      <Typography variant="subtitle1" gutterBottom>
        No items found for {title}
      </Typography>
    );
  }

  return (
    <Box sx={{ mb: 2 }}>
      <Typography variant="subtitle1" gutterBottom>
        {title}
      </Typography>
      <Box sx={{ border: '1px solid #ddd', p: 2 }}>
        <ul style={{ margin: 0, paddingLeft: 20 }}>
          {data.map((item, index) => (
            <li key={index}>{item}</li>
          ))}
        </ul>
      </Box>
    </Box>
  );
}

// Fields to display in merged results.
const MERGED_COLUMNS = [
  "id",
  "name",
  "price",
  "room_type",
  "listing_url",
  "description"
];

function filterColumns(dataArray, desiredFields) {
  if (!Array.isArray(dataArray)) return [];
  return dataArray.map(item => {
    const filtered = {};
    desiredFields.forEach(field => {
      if (item[field] !== undefined) {
        filtered[field] = item[field];
      }
    });
    return filtered;
  });
}

function App() {
  const [operation, setOperation] = useState('query');
  const [query, setQuery] = useState('');
  const [database, setDatabase] = useState('mysql');
  const [result, setResult] = useState('');
  const [loading, setLoading] = useState(false);
  const [showRawResults, setShowRawResults] = useState(true);
  const [showRawOutput, setShowRawOutput] = useState(false);
  const [showMySQL, setShowMySQL] = useState(true);
  const [showMongo, setShowMongo] = useState(true);
  const [showFirebase, setShowFirebase] = useState(true);
  const [activeTab, setActiveTab] = useState(0);
  const [modifyResult, setModifyResult] = useState(null);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setModifyResult(null);
    try {
      let response;
      if (operation === 'query') {
        response = await axios.post('http://localhost:8000/query', {
          query,
          db_type: database
        });
      } else if (operation === 'explore') {
        response = await axios.post('http://localhost:8000/explore', {
          query,
          db_type: database
        });
      } else if (operation === 'modify') {
        response = await axios.post('http://localhost:8000/modify', {
          modification: query,
          db_type: database
        });
        setModifyResult(response.data);
      }
      setResult(response.data);
    } catch (error) {
      setResult({ error: error.message });
    } finally {
      setLoading(false);
    }
  };

  const handleClear = () => {
    setResult(null);
    setQuery('');
    setModifyResult(null);
  };

  // Parse different types of responses
  const isExploration = operation === 'explore' && result && result.exploration_type;
  const explorationData = isExploration ? result : null;
  
  // Regular query response handling
  const converted = !isExploration && result ? result.converted_queries : null;
  const results = !isExploration && result ? result.results : null;
  const { mysql, mongodb, firebase } = results || {};

  // Modified result checking logic
  const hasMySQL = mysql && (Array.isArray(mysql) ? mysql.length > 0 : true);
  const hasMongoDB = mongodb && (typeof mongodb === 'object' && Object.keys(mongodb).length > 0);
  const hasFirebase = firebase && (typeof firebase === 'object' && Object.keys(firebase).length > 0);

  // Debug logging
  console.log('Result:', result);
  console.log('Results:', results);
  console.log('MySQL:', mysql);
  console.log('MongoDB:', mongodb);
  console.log('Firebase:', firebase);

  // Render exploration content based on result type
  const renderExplorationContent = () => {
    if (!explorationData) return null;

    const { exploration_type, message, data, db_type } = explorationData;

    switch (exploration_type) {
      case 'tables':
      case 'collections':
      case 'nodes':
        return <ListDisplay data={data} title={message} />;
      
      case 'schema':
        if (db_type === 'firebase') {
          return <StructureDisplay data={data} title={message} />;
        }
        return <SchemaDisplay data={data} title={message} />;
      
      case 'sample_data':
        return <DataTable data={data} title={message} />;
      
      case 'error':
        return (
          <Typography color="error" variant="body1">
            {message}
          </Typography>
        );
      
      default:
        // If we get regular query results through exploration endpoint
        if (data && Array.isArray(data)) {
          return <DataTable data={data} title="Exploration Results" />;
        }
        return (
          <pre style={{ backgroundColor: '#f5f5f5', padding: '8px', overflowX: 'auto' }}>
            {JSON.stringify(explorationData, null, 2)}
          </pre>
        );
    }
  };

  const getExampleQueries = () => {
    switch (operation) {
      case 'explore':
        return [
          "What tables are in the database?",
          "Show me the schema of the Listings table",
          "What columns are in the Reviews table?",
          "Show me 5 sample rows from the Hosts table",
          "What collections are in MongoDB?",
          "Show me the structure of the Firebase listings node"
        ];
      case 'modify':
        return [
          "Add a new listing with name 'Luxury Apartment' and price 200",
          "Delete the listing with id 123",
          "Update the price of listing with id 456 to 300",
          "Insert a new review for listing 789 with rating 5"
        ];
      case 'query':
      default:
        return [
          "Show me listings with more than 50 reviews",
          "Find all listings with price less than $100",
          "Get listings in Manhattan with 2 or more bedrooms",
          "Show me the most expensive listings"
        ];
    }
  };

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      {/* Query/Exploration Form Section */}
      <Paper elevation={3} sx={{ p: 3, mb: 3 }}>
        <Typography variant="h5" gutterBottom>
          ChatDB - Natural Language Interface
        </Typography>
        <form onSubmit={handleSubmit}>
          <Box sx={{ mb: 2 }}>
            <FormControl fullWidth>
              <InputLabel>Operation Type</InputLabel>
              <Select
                value={operation}
                label="Operation Type"
                onChange={(e) => setOperation(e.target.value)}
              >
                <MenuItem value="query">Query</MenuItem>
                <MenuItem value="explore">Explore Schema</MenuItem>
                <MenuItem value="modify">Modify Data</MenuItem>
              </Select>
            </FormControl>
          </Box>

          <Box sx={{ mb: 2 }}>
            <FormControl fullWidth>
              <InputLabel>Database</InputLabel>
              <Select
                value={database}
                label="Database"
                onChange={(e) => setDatabase(e.target.value)}
              >
                <MenuItem value="mysql">MySQL</MenuItem>
                <MenuItem value="mongodb">MongoDB</MenuItem>
                <MenuItem value="firebase">Firebase</MenuItem>
              </Select>
            </FormControl>
          </Box>

          <Box sx={{ mb: 2 }}>
            <TextField
              fullWidth
              multiline
              rows={4}
              label={
                operation === 'query'
                  ? "Enter your query in natural language"
                  : operation === 'explore'
                  ? "Enter your schema exploration question"
                  : "Enter your data modification request"
              }
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder={
                operation === 'query'
                  ? "e.g., Show me the listings with the highest number of reviews"
                  : operation === 'explore'
                  ? "e.g., What tables are in the database? or Show me the schema of Listings table"
                  : "e.g., Add a new listing with name 'Luxury Apartment' and price 200"
              }
            />
          </Box>

          <Box sx={{ mb: 1 }}>
            <Typography variant="body2" color="textSecondary">
              Example questions:
            </Typography>
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mt: 0.5 }}>
              {getExampleQueries().map((example, i) => (
                <Button 
                  key={i} 
                  size="small" 
                  variant="outlined" 
                  onClick={() => setQuery(example)}
                  sx={{ mb: 1 }}
                >
                  {example}
                </Button>
              ))}
            </Box>
          </Box>

          <Box sx={{ display: 'flex', gap: 2 }}>
            <Button
              variant="contained"
              type="submit"
              disabled={loading || !query}
              fullWidth
            >
              {loading ? 'Processing...' : (
                operation === 'query' ? 'Execute Query' :
                operation === 'explore' ? 'Explore' :
                'Modify Data'
              )}
            </Button>
            <Button
              variant="outlined"
              color="error"
              onClick={handleClear}
              disabled={loading || (!query && !result)}
              fullWidth
            >
              Clear All
            </Button>
          </Box>
        </form>
      </Paper>

      {/* Results Section */}
      {result && (
        <Paper elevation={3} sx={{ p: 3, mb: 3 }}>
          <Typography variant="h6" gutterBottom>
            {operation === 'explore' ? 'Schema Exploration Results' : 
             operation === 'modify' ? 'Modification Results' : 'Query Results'}
          </Typography>

          {operation === 'modify' && modifyResult ? (
            <Box sx={{ mb: 3 }}>
              <Typography 
                variant="subtitle1" 
                color={modifyResult.success ? 'success.main' : 'error.main'}
                gutterBottom
              >
                {modifyResult.message || "Modification processed"}
              </Typography>
              <Box sx={{ mb: 3 }}>
                <Typography variant="subtitle1" gutterBottom>
                  Converted Modifications
                </Typography>
                <pre style={{ backgroundColor: '#f5f5f5', padding: '8px', overflowX: 'auto' }}>
                  {JSON.stringify(modifyResult.converted_modifications, null, 2)}
                </pre>
              </Box>
              {modifyResult.results && (
                <Box sx={{ mb: 3 }}>
                  <Typography variant="subtitle1" gutterBottom>
                    Modification Results
                  </Typography>
                  <pre style={{ backgroundColor: '#f5f5f5', padding: '8px', overflowX: 'auto' }}>
                    {JSON.stringify(modifyResult.results, null, 2)}
                  </pre>
                </Box>
              )}
            </Box>
          ) : isExploration ? (
            renderExplorationContent()
          ) : (
            <>
              {/* Toggle for Converted Queries and Raw Output */}
              <Box sx={{ mb: 2, display: 'flex', justifyContent: 'flex-end', gap: 2 }}>
                <Button
                  variant="text"
                  onClick={() => setShowRawResults(!showRawResults)}
                >
                  {showRawResults ? 'Hide Converted Queries' : 'Show Converted Queries'}
                </Button>
                <Button
                  variant="text"
                  onClick={() => setShowRawOutput(!showRawOutput)}
                >
                  {showRawOutput ? 'Hide Raw Output' : 'Show Raw Output'}
                </Button>
              </Box>

              {showRawResults && converted && (
                <Box sx={{ mb: 3 }}>
                  <Typography variant="subtitle1" gutterBottom>
                    Converted Queries
                  </Typography>
                  <pre style={{ backgroundColor: '#f5f5f5', padding: '8px', overflowX: 'auto' }}>
                    {JSON.stringify(converted, null, 2)}
                  </pre>
                </Box>
              )}
              
              {showRawOutput && (
                <Box sx={{ mb: 3 }}>
                  <Typography variant="subtitle1" gutterBottom>
                    Raw Output
                  </Typography>
                  <pre style={{ backgroundColor: '#f5f5f5', padding: '8px', overflowX: 'auto' }}>
                    {JSON.stringify(result, null, 2)}
                  </pre>
                </Box>
              )}

              <Typography variant="subtitle1" gutterBottom>
                Individual Database Results
              </Typography>
              {/* MySQL Result */}
              {database === 'mysql' && (
                <Accordion expanded={showMySQL} onChange={() => setShowMySQL(!showMySQL)}>
                  <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                    <Typography variant="subtitle1">MySQL Results</Typography>
                  </AccordionSummary>
                  <AccordionDetails>
                    {hasMySQL ? (
                      <>
                        {converted && converted.mysql && (
                          <Box sx={{ mb: 2 }}>
                            <Typography variant="subtitle2" gutterBottom>Executed Query:</Typography>
                            <Paper elevation={0} sx={{ p: 2, bgcolor: '#f8f9fa', mb: 2 }}>
                              <code>{converted.mysql}</code>
                            </Paper>
                          </Box>
                        )}
                        <DataTable data={Array.isArray(mysql) ? mysql : [mysql]} title="MySQL" />
                      </>
                    ) : (
                      <Typography variant="body2">No results from MySQL.</Typography>
                    )}
                  </AccordionDetails>
                </Accordion>
              )}

              {/* MongoDB Result */}
              {database === 'mongodb' && (
                <Accordion expanded={showMongo} onChange={() => setShowMongo(!showMongo)}>
                  <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                    <Typography variant="subtitle1">MongoDB Results</Typography>
                  </AccordionSummary>
                  <AccordionDetails>
                    {hasMongoDB ? (
                      <>
                        {converted && converted.mongodb && (
                          <Box sx={{ mb: 2 }}>
                            <Typography variant="subtitle2" gutterBottom>Executed Query:</Typography>
                            <Paper elevation={0} sx={{ p: 2, bgcolor: '#f8f9fa', mb: 2 }}>
                              <pre style={{ margin: 0 }}>
                                {typeof converted.mongodb === 'string' 
                                  ? converted.mongodb 
                                  : JSON.stringify(converted.mongodb, null, 2)}
                              </pre>
                            </Paper>
                          </Box>
                        )}
                        <DataTable 
                          data={Array.isArray(mongodb) ? mongodb : [mongodb]} 
                          title="MongoDB" 
                        />
                      </>
                    ) : (
                      <Typography variant="body2">No results from MongoDB.</Typography>
                    )}
                  </AccordionDetails>
                </Accordion>
              )}

              {/* Firebase Result */}
              {database === 'firebase' && (
                <Accordion expanded={showFirebase} onChange={() => setShowFirebase(!showFirebase)}>
                  <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                    <Typography variant="subtitle1">Firebase Results</Typography>
                  </AccordionSummary>
                  <AccordionDetails>
                    {hasFirebase ? (
                      <>
                        {converted && converted.firebase && (
                          <Box sx={{ mb: 2 }}>
                            <Typography variant="subtitle2" gutterBottom>Executed Query:</Typography>
                            <Paper elevation={0} sx={{ p: 2, bgcolor: '#f8f9fa', mb: 2 }}>
                              <pre style={{ margin: 0 }}>
                                {typeof converted.firebase === 'string' 
                                  ? converted.firebase 
                                  : JSON.stringify(converted.firebase, null, 2)}
                              </pre>
                            </Paper>
                          </Box>
                        )}
                        <pre style={{ backgroundColor: '#f5f5f5', padding: '8px' }}>
                          {JSON.stringify(firebase, null, 2)}
                        </pre>
                      </>
                    ) : (
                      <Typography variant="body2">No results from Firebase.</Typography>
                    )}
                  </AccordionDetails>
                </Accordion>
              )}
            </>
          )}
        </Paper>
      )}
    </Container>
  );
}

export default App;

