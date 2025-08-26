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
      variant='outlined'
      startIcon={<CloseIcon />}
      sx={{
        '&:hover': {
          backgroundColor: 'primary.main',
          color: 'white',
        },
      }}
    ></Button>
  ) : (
    <Button
      onClick={() => setFaq('open')}
      variant='outlined'
      startIcon={<HelpIcon />}
      sx={{
        '&:hover': {
          backgroundColor: 'primary.main',
          color: 'white',
        },
      }}
    ></Button>
  );
}
