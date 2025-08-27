import React from 'react';
import { Box, Typography } from '@mui/material';

export default function FAQ() {
  return (
    <Box sx={{ 
      pb: '10vh', 
      px: { xs: 2, sm: 3, md: 0 },
      mx: { xs: 0, md: 'auto' }
    }}>
      <Typography
        variant='h6'
        component='h2'
        sx={{
          fontFamily: 'Big Shoulders, sans-serif',
          fontWeight: 700,
          marginTop: 2,
          mb: 3,
          fontSize: { xs: '1.1rem', sm: '1.25rem', md: '1.5rem' }
        }}
      >
        Frequently Asked Questions
      </Typography>

      <Typography
        variant='h6'
        component='h3'
        sx={{
          fontFamily: 'Big Shoulders, sans-serif',
          fontWeight: 600,
          marginTop: 3,
          mb: 1,
        }}
      >
        What am I looking at?
      </Typography>
      <Typography sx={{ mb: 2 }}>
        Inspired by{' '}
        <a
          href='https://github.com/zack'
          target='_blank'
          rel='noopener noreferrer'
          style={{ color: '#1976d2', textDecoration: 'underline' }}
        >
          Zack Youngren
        </a>{' '}
        and his wonderful project{' '}
        <a
          href='https://www.citibikedata.nyc/?view=borough'
          target='_blank'
          rel='noopener noreferrer'
          style={{ color: '#1976d2', textDecoration: 'underline' }}
        >
          citibikedata.nyc
        </a>
        , this is an attempt to track the usage of Chicago's bikeshare program,
        Divvy, across the 77 community areas.
      </Typography>

      <Typography
        variant='h6'
        component='h3'
        sx={{
          fontFamily: 'Big Shoulders, sans-serif',
          fontWeight: 600,
          marginTop: 3,
          mb: 1,
        }}
      >
        Where does the data come from?
      </Typography>
      <Typography sx={{ mb: 2 }}>
        Divvy publishes trip data monthly to a{' '}
        <a
          href='https://divvybikes.com/system-data'
          target='_blank'
          rel='noopener noreferrer'
          style={{ color: '#1976d2', textDecoration: 'underline' }}
        >
          public S3 bucket
        </a>
        .
      </Typography>

      <Typography
        variant='h6'
        component='h3'
        sx={{
          fontFamily: 'Big Shoulders, sans-serif',
          fontWeight: 600,
          marginTop: 3,
          mb: 1,
        }}
      >
        What is the time frame of the data?
      </Typography>
      <Typography sx={{ mb: 2 }}>
        Right now, the data is only from January 2020 to July 2025. This is a
        work in progress as I try and figure out the most efficient way to parse
        through such a large dataset.
      </Typography>

      <Typography
        variant='h6'
        component='h3'
        sx={{
          fontFamily: 'Big Shoulders, sans-serif',
          fontWeight: 600,
          marginTop: 3,
          mb: 1,
        }}
      >
        What are you counting?
      </Typography>
      <Typography sx={{ mb: 2 }}>
        Each arrival and departure is counted as one trip.
      </Typography>

      <Typography
        variant='h6'
        component='h3'
        sx={{
          fontFamily: 'Big Shoulders, sans-serif',
          fontWeight: 600,
          marginTop: 3,
          mb: 1,
        }}
      >
        Where can I find the code for this?
      </Typography>
      <Typography sx={{ mb: 2 }}>
        <a
          href='https://github.com/liam-connolly/divvy-tracking'
          target='_blank'
          rel='noopener noreferrer'
          style={{ color: '#1976d2', textDecoration: 'underline' }}
        >
          Here
        </a>
        . If you have any questions, please feel free to reach out to me at{' '}
        <a
          href='mailto:liamconnolly.hello@gmail.com'
          target='_blank'
          rel='noopener noreferrer'
          style={{ color: '#1976d2', textDecoration: 'underline' }}
        >
          liamconnolly.hello@gmail.com
        </a>
        .
      </Typography>
    </Box>
  );
}
