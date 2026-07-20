import React from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
// Shared styles load FIRST (before the component graph) so every
// per-component .css can override them deterministically.
import './styles/base.css'
import './styles/modal.css'
import './styles/badges.css'
import EntryScreen from './EntryScreen'
import SystemPage from './SystemPage'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<EntryScreen />} />
        <Route path="/systems/:systemId" element={<SystemPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </BrowserRouter>
  </React.StrictMode>,
)
