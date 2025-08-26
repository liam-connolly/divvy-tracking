'use client';

import React from 'react';
import { Button, Typography } from '@mui/material';
import { Help as HelpIcon, Close as CloseIcon } from '@mui/icons-material';
import { useQueryState } from 'nuqs';

export default function FAQButton() {
  const [faq, setFaq] = useQueryState('faq');

  return faq ? (
    <Button
      onClick={() => setFaq(null)}
      sx={{
        flexDirection: 'column',
        color: 'black',
        border: 'none',
        '&:hover': {
          backgroundColor: 'black',
          color: 'white',
        },
      }}
    >
      <CloseIcon />
      <Typography>Back</Typography>
    </Button>
  ) : (
    <Button
      onClick={() => setFaq('open')}
      sx={{
        flexDirection: 'column',
        color: 'black',
        border: 'none',
        '&:hover': {
          backgroundColor: 'black',
          color: 'white',
        },
      }}
    >
      <HelpIcon />
      <Typography>FAQ</Typography>
    </Button>
  );
}
