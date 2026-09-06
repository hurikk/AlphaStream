# Getting Started

### Requirements
- Python 3.12+
- Docker
- [uv](https://docs.astral.sh/uv/)


### Configuration

1. Clone the repository
```bash
git clone https://github.com/hurikk/AlphaStream.git
cd AlphaStream
```

2. Copy the environment variables file
```bash
cp .env.example .env
```
> Fill the `.env` file with your values. Ex:<br>
> DB_HOST=localhost<br>
> DB_PORT=5432<br>
> DB_USER=admin<br>
> DB_PASSWORD=123456<br>
> OBS: `DB_NAME` must be stock_database (DB_PASSWORD=stock_database)

3. Upload the database
```bash
cd .\docker\
docker compose --env-file ../.env up -d
cd ..
```

4. Install the dependencies
```bash
uv sync
```

5. Run the project
```bash
uv run python -m src.alphastream.pipelines.main
```
