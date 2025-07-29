import * as React from 'react';
import { Button, Icon, Stack, Typography } from '@mui/material';
import HelpIcon from '@mui/icons-material/Help';

export default function Header() {
  return (
    <Stack
      direction={'row'}
      justifyContent={'space-between'}
      alignItems={'center'}
    >
      <Typography variant='h3'>Divvy Bike Data</Typography>
      <Button variant='outlined' sx={{ flexDirection: 'column' }}>
        <HelpIcon />
        <Typography variant='h6'>FAQ</Typography>
      </Button>
    </Stack>
  );
}
