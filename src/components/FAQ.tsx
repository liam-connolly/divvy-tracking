import React from 'react';
import { Box, Typography } from '@mui/material';

export default function FAQ() {
  return (
    <Box sx={{ pb: '10vh' }}>
      <Typography
        variant='h5'
        component='h2'
        sx={{
          fontFamily: 'Big Shoulders, sans-serif',
          fontWeight: 700,
          marginTop: 2,
          mb: 3,
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
        Divvy publishes trip data monthly to their{' '}
        <a
          href='https://divvybikes.com/system-data'
          target='_blank'
          rel='noopener noreferrer'
          style={{ color: '#1976d2', textDecoration: 'underline' }}
        >
          system data page
        </a>
        . I download this data, process it to calculate station usage
        statistics, and store it in a database for analysis.
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
        I'm counting each bike departure and arrival as separate events. This
        gives the most accurate picture of how busy each station is throughout
        the day. Most trips generate one departure from the start station and
        one arrival at the end station.
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
        How are Community Areas determined?
      </Typography>
      <Typography sx={{ mb: 2 }}>
        Chicago is divided into 77 official Community Areas as defined by the
        city. Each Divvy station is mapped to its Community Area based on its
        geographic location. You can learn more about Community Areas on the{' '}
        <a
          href='https://www.chicago.gov/city/en/depts/dcd/supp_info/community_areas.html'
          target='_blank'
          rel='noopener noreferrer'
          style={{ color: '#1976d2', textDecoration: 'underline' }}
        >
          City of Chicago website
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
        How far back does the data go?
      </Typography>
      <Typography sx={{ mb: 2 }}>
        Divvy has been publishing trip data since the system launched in 2013.
        This dashboard includes data from the most recent available datasets.
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
        Why might some stations show different names or locations?
      </Typography>
      <Typography sx={{ mb: 2 }}>
        Divvy occasionally moves stations or updates their names as the system
        evolves. The data reflects the station information as it was recorded at
        the time of each trip. Some historical inconsistencies may exist due to
        these operational changes.
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
        If you notice any issues with the data or have ideas for improvements,
        please feel free to reach out or contribute to the project.
      </Typography>
    </Box>
  );
}
