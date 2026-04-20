docker network create market
docker volume create MarketData
docker compose -f docker-compose.yml up --build -d