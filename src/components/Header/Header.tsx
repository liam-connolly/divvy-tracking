import * as React from 'react';
import { Box, Button, Icon, Stack, Typography } from '@mui/material';
import HelpIcon from '@mui/icons-material/Help';
import FAQButton from '../FAQButton';
import { exoFontFamily } from '@/app/ThemeProvider';

export default function Header() {
  return (
    <div
      style={{
        display: 'flex',
        justifyContent: 'space-between',
        marginTop: 2,
      }}
    >
      <Typography
        variant='h4'
        component='h1'
        sx={{
          fontFamily: exoFontFamily,
          display: 'flex-item',
        }}
      >
        Divvy Station Bike Data
      </Typography>
      <Box sx={{ display: 'flex-item', mb: 1 }}>
        <React.Suspense>
          <FAQButton />
        </React.Suspense>
      </Box>
    </div>
  );
}
