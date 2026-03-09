#!/bin/bash

echo "🚀 Starting deployment..."

echo "📥 Pulling latest code..."
git pull

echo "📦 Installing dependencies..."
npm install

echo "🔁 Restarting FormBot..."
pm2 restart FormBot --update-env

echo "📊 PM2 Status:"
pm2 list

echo "📜 Showing recent logs..."
pm2 logs FormBot --lines 20
