import React from 'react'
import ReactDOM from 'react-dom/client'
// Shared styles load FIRST (before App's component graph) so every
// per-component .css can override them deterministically.
import './styles/base.css'
import './styles/modal.css'
import './styles/badges.css'
import App from './App'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
