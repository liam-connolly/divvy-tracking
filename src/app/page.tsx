import ContentContainer from '@/components/ContentContainer';
import FAQButton from '@/components/FAQButton';
import React from 'react';
import { Box, Grid, Typography } from '@mui/material';

export default function Home() {
  return (
    <main>
      <Grid
        container
        justifyContent='center'
        columns={{ xs: 6, sm: 8, md: 8, lg: 12 }}
        height='90vh'
        width='97vw'
      >
        <Grid
          size={{ xs: 6 }}
          sx={{
            height: '100%',
            display: 'flex',
            alignItems: 'stretch',
            flexDirection: 'column',
          }}
        >
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              marginTop: 16,
              marginBottom: 24,
            }}
          >
            <Typography
              variant='h4'
              component='h1'
              className='font-big-shoulders'
              sx={{
                fontWeight: 700,
                color: 'text.primary',
              }}
            >
              <span style={{ color: '#E4002B', marginRight: '8px' }}>✶</span>
              Divvy Analytics Dashboard
            </Typography>

            <Box sx={{ display: 'flex-item' }}>
              <React.Suspense>
                <FAQButton />
              </React.Suspense>
            </Box>
          </div>

          <React.Suspense
            fallback={
              <Box
                sx={{
                  display: 'flex',
                  justifyContent: 'center',
                  alignItems: 'center',
                  height: '50vh',
                }}
              >
                <div className='text-center'>
                  <div className='animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mx-auto mb-4'></div>
                  <p className='text-gray-600'>Loading Divvy data...</p>
                </div>
              </Box>
            }
          >
            <ContentContainer />
          </React.Suspense>
        </Grid>
      </Grid>
    </main>
  );
}
