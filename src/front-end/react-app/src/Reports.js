// src/TaxiTripsTable.js

import React, { useEffect, useState } from 'react';
import { Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Paper, Container, Typography } from '@mui/material';


const ReportOne = () => {
  const [data, setData] = useState([{}]);

  useEffect(() => {
    fetch('/api/reqone')
      .then(response => response.json())
      .then(
          data => {
            setData(data) 
          })
      .catch(error => console.error('Error fetching data:', error));
  }, []);

  return (
    <Container>
      <Typography variant="h4" gutterBottom>
        Weekly Taxi Trips and Covid Cases by Zipcode (2020) 
      </Typography>
      <TableContainer component={Paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Week Number</TableCell>
              <TableCell>Community Area</TableCell>
              <TableCell>Zipcode</TableCell>
              <TableCell>Trip Count</TableCell>
              <TableCell>Covid Cases</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {data.map((row, index) => (
              <TableRow key={index}>
                <TableCell>{row.week_number}</TableCell>
                <TableCell>{row.community_area}</TableCell>
                <TableCell>{row.zipcode}</TableCell>
                <TableCell>{row.tripcount}</TableCell>
                <TableCell>{row.covid_cases}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Container>
  );
};

const ReportTwo = () => {
  const [data, setData] = useState([{}]);

  useEffect(() => {
    fetch('/api/reqtwo')
      .then(response => response.json())
      .then(
          data => {
            setData(data) 
          })
      .catch(error => console.error('Error fetching data:', error));
  }, []);

  return (
    <Container>
       <Typography variant="h4" gutterBottom>
       Taxi trips from O’Hare and Midway Airports
      </Typography>
      <TableContainer component={Paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Drop-off Community Area</TableCell>
              <TableCell>Drop-off Zipcode</TableCell>
              <TableCell>Trip Count</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {data.map((row, index) => (
              <TableRow key={index}>
                <TableCell>{row.dropoff_community_area}</TableCell>
                <TableCell>{row.dropoff_zip_code}</TableCell>
                <TableCell>{row.trip_cnt}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Container>
  );
};

const ReportThree = () => {
  const [data, setData] = useState([{}]);

  useEffect(() => {
    fetch('/api/reqthree')
      .then(response => response.json())
      .then(
          data => {
            setData(data) 
          })
      .catch(error => console.error('Error fetching data:', error));
  }, []);

  return (
    <Container>
      <Typography variant="h4" gutterBottom>
      Weekly Taxi Trips from/to High-CCVI Neighborhoods (2020) 
      </Typography>
      <TableContainer component={Paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Week Number</TableCell>
              <TableCell>Community Area</TableCell>
              <TableCell>Zipcode</TableCell>
              <TableCell>Trip Count</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {data.map((row, index) => (
              <TableRow key={index}>
                <TableCell>{row.week_number}</TableCell>
                <TableCell>{row.community_area}</TableCell>
                <TableCell>{row.zipcode}</TableCell>
                <TableCell>{row.trip_count}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Container>
  );
};

const ReportFour = () => {
  const [data, setData] = useState([{}]);

  useEffect(() => {
    fetch('/api/reqfour')
      .then(response => response.json())
      .then(
          data => {
            setData(data) 
          })
      .catch(error => console.error('Error fetching data:', error));
  }, []);

  return (
    <Container>
      <Typography variant="h4" gutterBottom>
      Weekly Traffic Patterns by Zipcode (2020) 
      </Typography>
      <TableContainer component={Paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Drop-off Zipcode</TableCell>
              <TableCell>Pick-up Zipcode</TableCell>
              <TableCell>Week Number</TableCell>
              <TableCell>Trip Count</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {data.map((row, index) => (
              <TableRow key={index}>
                <TableCell>{row.dropoff_zip_code}</TableCell>
                <TableCell>{row.pickup_zip_code}</TableCell>
                <TableCell>{row.week_number}</TableCell>
                <TableCell>{row.trip_count}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Container>
  );
};

const ReportFive = () => {
  const [data, setData] = useState([{}]);

  useEffect(() => {
    fetch('/api/reqfive')
      .then(response => response.json())
      .then(
          data => {
            setData(data) 
          })
      .catch(error => console.error('Error fetching data:', error));
  }, []);

  return (
    <Container>
       <Typography variant="h4" gutterBottom>
       Building Permit Fees in High Unemployment and Poverty Neighborhoods 
      </Typography>
      <TableContainer component={Paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Community Area Name</TableCell>
              <TableCell>Community Area</TableCell>
              <TableCell>Below Poverty Level</TableCell>
              <TableCell>Unemployment</TableCell>
              <TableCell>Total Fee</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {data.map((row, index) => (
              <TableRow key={index}>
                <TableCell>{row.community_area_name}</TableCell>
                <TableCell>{row.community_area}</TableCell>
                <TableCell>{row.below_poverty_level}</TableCell>
                <TableCell>{row.unemployment}</TableCell>
                <TableCell>{row.total_fee}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Container>
  );
};

const ReportSix = () => {
  const [data, setData] = useState([{}]);

  useEffect(() => {
    fetch('/api/reqsix')
      .then(response => response.json())
      .then(
          data => {
            setData(data) 
          })
      .catch(error => console.error('Error fetching data:', error));
  }, []);

  return (
    <Container>
      <Typography variant="h4" gutterBottom>
      Eligible Neighborhoods for the Small Business Emergency Loan
      </Typography>
      <TableContainer component={Paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Community Area Name</TableCell>
              <TableCell>Community Area</TableCell>
              <TableCell>Permit Count</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {data.map((row, index) => (
              <TableRow key={index}>
                <TableCell>{row.community_area_name}</TableCell>
                <TableCell>{row.community_area}</TableCell>
                <TableCell>{row.permit_count}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Container>
  );
};



export { ReportOne, ReportTwo, ReportThree, ReportFour, ReportFive, ReportSix };
