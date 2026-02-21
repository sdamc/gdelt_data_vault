#!/bin/bash
# Setup script to ensure Ollama model is available

echo "Checking Ollama service..."
until curl -s http://ollama:11434/api/tags > /dev/null 2>&1; do
    echo "Waiting for Ollama service to start..."
    sleep 2
done

echo "✓ Ollama is running"

# Pull the model if not already available
echo "Ensuring llama3.2:3b model is available..."
docker exec ollama ollama pull llama3.2:3b

echo "✓ Model ready"
echo ""
echo "To run AI insights generation:"
echo "  docker exec -it airflow python /workspace/ingestion/ai_news_insights.py"
