// // src/App.js

// import React, {useState, useEffect } from 'react';
// import TaxiTripsTable from './ReqThree.js';
// import { CssBaseline, AppBar, Toolbar, Typography } from '@mui/material';

// function App() {
//   return (
//     <>
//       <CssBaseline />
//       <AppBar position="static">
//         <Toolbar>
//           <Typography variant="h6">
//           Chicago Business Intelligence for Strategic Planning
//           </Typography>
//         </Toolbar>
//       </AppBar>
//       <TaxiTripsTable />
//     </>
//   );
// }

// export default App;

// src/App.js

import React from 'react';
import { BrowserRouter as Router, Route, Routes, Link } from 'react-router-dom';
import  { ReportOne, ReportTwo, ReportThree, ReportFour, ReportFive, ReportSix } from './Reports';
import { CssBaseline, AppBar, Toolbar, Typography, Container } from '@mui/material'; 

const Home = () => (
  <Container>
    <Typography variant="h4" gutterBottom>
    Chicago Business Intelligence for Strategic Planning
    </Typography>
    <Link to="/report-one" style={{ textDecoration: 'none' }}>
      <Typography variant="h6" color="primary">
      Weekly Taxi Trips and Covid Cases by Zipcode (Requirement 1)
      </Typography>
    </Link>
    <Link to="/report-two" style={{ textDecoration: 'none' }}>
      <Typography variant="h6" color="primary">
      Taxi trips from O’Hare and Midway Airports (Requirement 2)
      </Typography>
    </Link>
    <Link to="/report-three" style={{ textDecoration: 'none' }}>
      <Typography variant="h6" color="primary">
      Weekly Taxi Trips from/to High-CCVI Neighborhoods (Requirement 3)
      </Typography>
    </Link>
    <Link to="/report-four" style={{ textDecoration: 'none' }}>
      <Typography variant="h6" color="primary">
        Weekly Traffic Patterns by Zipcode (Requirement 4)
      </Typography>
    </Link>
    <Link to="/report-five" style={{ textDecoration: 'none' }}>
      <Typography variant="h6" color="primary">
      Building Permit Fees in High Unemployment and Poverty Neighborhoods (Requirement 5)
      </Typography>
    </Link>
    <Link to="/report-six" style={{ textDecoration: 'none' }}>
      <Typography variant="h6" color="primary">
      Eligible Neighborhoods for the Small Business Emergency Loan (Requirement 6)
      </Typography>
    </Link>
  </Container>
);

function App() {
  return (
    <Router>
      <CssBaseline />
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/report-one" element={<ReportOne />} />
        <Route path="/report-two" element={<ReportTwo />} />
        <Route path="/report-three" element={<ReportThree />} />
        <Route path="/report-four" element={<ReportFour />} />
        <Route path="/report-five" element={<ReportFive />} />
        <Route path="/report-six" element={<ReportSix />} />
      </Routes>
    </Router>
  );
}

export default App;
