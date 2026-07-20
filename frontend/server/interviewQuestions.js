// The Interview-mode question bank: classic system-design interview prompts, adapted
// from the public question lists of hellointerview.com and systemdesign.io (each entry
// carries its source attribution).
//
// Statements are deliberately ONE minimal sentence — the whole point of the interview
// flow is that the candidate derives the functional requirements themselves, so the
// prompt must not leak them. The interviewer session receives the picked entry inlined
// in its system prompt (interview.js) and opens the interview with it.
//
// Pure data module — no plugin, no routes. interview.js imports pickQuestion().

const HELLOINTERVIEW = {
  name: 'HelloInterview',
  url: 'https://www.hellointerview.com/learn/system-design/problem-breakdowns',
}
const SYSTEMDESIGN_IO = {
  name: 'systemdesign.io',
  url: 'https://systemdesign.io',
}

export const QUESTIONS = [
  // --- adapted from HelloInterview's common-problem breakdowns -------------------
  { id: 'ticketmaster', title: 'Design Ticketmaster', source: HELLOINTERVIEW,
    statement: 'Design an online platform where people find and buy tickets to live events (concerts, sports, theater).' },
  { id: 'uber', title: 'Design Uber', source: HELLOINTERVIEW,
    statement: 'Design a ride-sharing service that connects riders who need a trip with nearby drivers.' },
  { id: 'bitly', title: 'Design Bit.ly', source: HELLOINTERVIEW,
    statement: 'Design a service that turns long URLs into short, shareable links.' },
  { id: 'dropbox', title: 'Design Dropbox', source: HELLOINTERVIEW,
    statement: 'Design a cloud file-storage service that lets people keep files in the cloud and reach them from any device.' },
  { id: 'youtube', title: 'Design YouTube', source: HELLOINTERVIEW,
    statement: 'Design a platform where people upload videos and watch what others have uploaded.' },
  { id: 'whatsapp', title: 'Design WhatsApp', source: HELLOINTERVIEW,
    statement: 'Design a messaging app people use to chat one-on-one and in groups.' },
  { id: 'web-crawler', title: 'Design a Web Crawler', source: HELLOINTERVIEW,
    statement: 'Design a system that continuously crawls the web and collects page content.' },
  { id: 'yelp', title: 'Design Yelp', source: HELLOINTERVIEW,
    statement: 'Design a service where people discover and review businesses near them.' },
  { id: 'online-judge', title: 'Design LeetCode', source: HELLOINTERVIEW,
    statement: 'Design a platform where people solve coding problems and submit solutions to be judged.' },
  { id: 'rate-limiter', title: 'Design a Distributed Rate Limiter', source: HELLOINTERVIEW,
    statement: 'Design a rate limiter other services can use to control how often each client may call them.' },
  { id: 'ad-click-aggregator', title: 'Design an Ad Click Aggregator', source: HELLOINTERVIEW,
    statement: 'Design a system that ingests ad click events and lets advertisers see how their ads perform.' },
  { id: 'news-feed', title: 'Design the Facebook News Feed', source: HELLOINTERVIEW,
    statement: 'Design the feed for a social network where people follow others and see their posts.' },
  { id: 'post-search', title: 'Design Post Search', source: HELLOINTERVIEW,
    statement: 'Design search over all the posts on a large social network.' },
  { id: 'top-k', title: 'Design a Top-K Leaderboard', source: HELLOINTERVIEW,
    statement: 'Design a system that reports the most popular items on a platform (say, the most-viewed videos).' },
  { id: 'tinder', title: 'Design Tinder', source: HELLOINTERVIEW,
    statement: 'Design a dating app where people browse profiles, swipe, and match.' },
  { id: 'strava', title: 'Design Strava', source: HELLOINTERVIEW,
    statement: 'Design an app where athletes record their runs and rides and share them.' },
  { id: 'live-comments', title: 'Design Live Comments', source: HELLOINTERVIEW,
    statement: 'Design the live comment stream that plays under a live video.' },
  { id: 'google-docs', title: 'Design Google Docs', source: HELLOINTERVIEW,
    statement: 'Design a collaborative editor where several people edit the same document at once.' },
  { id: 'robinhood', title: 'Design Robinhood', source: HELLOINTERVIEW,
    statement: 'Design a stock-trading app for retail investors.' },
  { id: 'payment-system', title: 'Design a Payment System', source: HELLOINTERVIEW,
    statement: 'Design a payment platform that lets online businesses charge their customers.' },
  { id: 'job-scheduler', title: 'Design a Distributed Job Scheduler', source: HELLOINTERVIEW,
    statement: 'Design a system that runs scheduled and recurring jobs across a fleet of machines.' },
  { id: 'online-auction', title: 'Design an Online Auction', source: HELLOINTERVIEW,
    statement: 'Design an auction site where sellers list items and buyers bid on them.' },
  { id: 'price-tracker', title: 'Design a Price Tracker', source: HELLOINTERVIEW,
    statement: 'Design a service that watches product prices on e-commerce sites for users.' },

  // --- adapted from systemdesign.io's question lists -----------------------------
  { id: 'twitter', title: 'Design Twitter', source: SYSTEMDESIGN_IO,
    statement: 'Design a social network where people post short messages and follow each other.' },
  { id: 'instagram', title: 'Design Instagram', source: SYSTEMDESIGN_IO,
    statement: 'Design a photo-sharing social app.' },
  { id: 'kv-store', title: 'Design a Key-Value Store', source: SYSTEMDESIGN_IO,
    statement: 'Design a distributed key-value store that applications use as a building block.' },
  { id: 'notification-system', title: 'Design a Notification System', source: SYSTEMDESIGN_IO,
    statement: 'Design a system that delivers notifications to users.' },
  { id: 'metrics-logging', title: 'Design a Metrics & Logging System', source: SYSTEMDESIGN_IO,
    statement: 'Design a system that collects metrics and logs from thousands of servers and makes them queryable.' },
  { id: 'blob-store', title: 'Design an Object Store', source: SYSTEMDESIGN_IO,
    statement: 'Design an object-storage service (like S3) where applications store and retrieve arbitrarily large files.' },
  { id: 'zoom', title: 'Design Zoom', source: SYSTEMDESIGN_IO,
    statement: 'Design a video-conferencing service.' },
  { id: 'hotel-booking', title: 'Design a Hotel Booking Service', source: SYSTEMDESIGN_IO,
    statement: 'Design a service where travelers search for hotel rooms and book them.' },
  { id: 'food-delivery', title: 'Design DoorDash', source: SYSTEMDESIGN_IO,
    statement: 'Design a food-delivery service that connects customers, restaurants, and couriers.' },
  { id: 'message-queue', title: 'Design a Message Queue', source: SYSTEMDESIGN_IO,
    statement: 'Design a distributed message queue that services use to talk to each other.' },
  { id: 'google-maps', title: 'Design Google Maps', source: SYSTEMDESIGN_IO,
    statement: 'Design a maps service that shows people where things are and how to get there.' },
  { id: 'gmail', title: 'Design Gmail', source: SYSTEMDESIGN_IO,
    statement: 'Design an email service.' },
  { id: 'distributed-cache', title: 'Design a Distributed Cache', source: SYSTEMDESIGN_IO,
    statement: 'Design a distributed in-memory cache that services put in front of their databases.' },
]

// Uniform random pick. `excludeId` (the previous interview's question, if any) is
// filtered out so back-to-back interviews never repeat the same question.
export function pickQuestion(excludeId) {
  const pool = QUESTIONS.filter((q) => q.id !== excludeId)
  const from = pool.length ? pool : QUESTIONS
  return from[Math.floor(Math.random() * from.length)]
}
