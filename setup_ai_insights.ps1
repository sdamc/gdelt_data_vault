# AI News Insights - Quick Setup Script
# Run this script to set up and test the AI insights feature

Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host "=" * 59 -ForegroundColor Cyan
Write-Host "  GDELT AI News Insights Generator - Setup" -ForegroundColor Cyan
Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host "=" * 59 -ForegroundColor Cyan
Write-Host ""

# Step 1: Check Docker services
Write-Host "[1/5] Checking Docker services..." -ForegroundColor Yellow
$services = docker-compose ps --services
if ($services -match "ollama") {
    Write-Host "  ✓ Ollama service found" -ForegroundColor Green
} else {
    Write-Host "  ✗ Ollama not in docker-compose. Starting services..." -ForegroundColor Red
    docker-compose up -d
    Start-Sleep -Seconds 10
}

# Step 2: Wait for Ollama to be ready
Write-Host "`n[2/5] Waiting for Ollama to start..." -ForegroundColor Yellow
$maxAttempts = 30
$attempt = 0
while ($attempt -lt $maxAttempts) {
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:11434/api/tags" -TimeoutSec 2 -ErrorAction SilentlyContinue
        if ($response.StatusCode -eq 200) {
            Write-Host "  ✓ Ollama is running" -ForegroundColor Green
            break
        }
    } catch {
        # Ignore and retry
    }
    $attempt++
    Start-Sleep -Seconds 2
    Write-Host "  ... attempt $attempt/$maxAttempts" -ForegroundColor Gray
}

if ($attempt -eq $maxAttempts) {
    Write-Host "  ✗ Ollama failed to start" -ForegroundColor Red
    exit 1
}

# Step 3: Pull Ollama model
Write-Host "`n[3/5] Downloading Llama 3.2 model (this may take a few minutes)..." -ForegroundColor Yellow
docker exec ollama ollama pull llama3.2:3b
if ($LASTEXITCODE -eq 0) {
    Write-Host "  ✓ Model downloaded" -ForegroundColor Green
} else {
    Write-Host "  ✗ Model download failed" -ForegroundColor Red
    exit 1
}

# Step 4: Create gold table
Write-Host "`n[4/5] Creating gold table..." -ForegroundColor Yellow
Set-Location transformation
dbt run --select fact_ai_news_insights
Set-Location ..
if ($LASTEXITCODE -eq 0) {
    Write-Host "  ✓ Gold table created" -ForegroundColor Green
} else {
    Write-Host "  ! Table may already exist (this is OK)" -ForegroundColor Yellow
}

# Step 5: Test query
Write-Host "`n[5/5] Testing database connection..." -ForegroundColor Yellow
$testQuery = "SELECT COUNT(*) FROM pg_vault.main_raw_vault.sat_news_tone WHERE event_datetime >= CURRENT_DATE - INTERVAL '7 days';"
$result = docker exec -it dv_postgres psql -U gdelt_user -d gdelt -t -c $testQuery
if ($LASTEXITCODE -eq 0) {
    Write-Host "  ✓ Found $($result.Trim()) articles from last 7 days" -ForegroundColor Green
} else {
    Write-Host "  ✗ Database query failed" -ForegroundColor Red
}

# Summary
Write-Host "`n" + ("=" * 60) -ForegroundColor Cyan
Write-Host "  Setup Complete!" -ForegroundColor Green
Write-Host ("=" * 60) -ForegroundColor Cyan
Write-Host ""
Write-Host "To generate AI insights, run:" -ForegroundColor White
Write-Host "  docker exec -it airflow python /workspace/ingestion/ai_news_insights.py" -ForegroundColor Cyan
Write-Host ""
Write-Host "To view results:" -ForegroundColor White
Write-Host "  docker exec -it dv_postgres psql -U gdelt_user -d gdelt -c `"SELECT category, continent, source_domain, ai_abstract FROM pg_vault.gold.fact_ai_news_insights LIMIT 5;`"" -ForegroundColor Cyan
Write-Host ""
Write-Host "See ingestion/AI_INSIGHTS_README.md for full documentation" -ForegroundColor Gray
Write-Host ""
