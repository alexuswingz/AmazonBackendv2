# AWS Deployment Guide for Forecast API

## Option 1: AWS Elastic Beanstalk (Recommended for Simplicity)

### Prerequisites
1. AWS Account with billing enabled
2. Install AWS CLI: https://aws.amazon.com/cli/
3. Install EB CLI: `pip install awsebcli`
4. Configure AWS credentials: `aws configure`

### Step-by-Step Deployment

```bash
# 1. Navigate to the project folder
cd forecast_app

# 2. Initialize Elastic Beanstalk application
eb init -p python-3.11 forecast-api --region us-east-1

# 3. Create the environment (this takes 5-10 minutes)
eb create forecast-api-prod --single

# 4. Deploy updates
eb deploy

# 5. Open the application in browser
eb open

# 6. Check status
eb status

# 7. View logs if needed
eb logs
```

### Environment Variables
Set these in the AWS Console under Configuration > Software:
- `SECRET_KEY`: Your production secret key
- `DATABASE_URL`: (Optional) PostgreSQL/MySQL connection string

### Estimated Costs
- **t3.micro** (free tier eligible): ~$0/month for first year
- **t3.small**: ~$15-20/month
- **t3.medium**: ~$30-40/month

---

## Option 2: AWS EC2 (More Control)

### Step-by-Step

```bash
# 1. Launch EC2 instance (Amazon Linux 2 or Ubuntu)
# Use AWS Console or CLI

# 2. SSH into the instance
ssh -i your-key.pem ec2-user@your-ip

# 3. Install Python and dependencies
sudo yum update -y
sudo yum install python3 python3-pip git -y

# 4. Clone your code (or upload via SCP)
git clone your-repo-url
cd forecast_app

# 5. Install requirements
pip3 install -r requirements.txt

# 6. Run with gunicorn
gunicorn application:application --bind 0.0.0.0:8000 --daemon

# 7. (Optional) Set up nginx as reverse proxy
sudo yum install nginx -y
# Configure nginx to proxy to port 8000
```

### Security Group
Open ports:
- 22 (SSH)
- 80 (HTTP)
- 443 (HTTPS)

---

## Option 3: AWS Lambda + API Gateway (Serverless)

### Requirements
- Install Zappa: `pip install zappa`

```bash
# 1. Initialize Zappa
zappa init

# 2. Deploy
zappa deploy production

# 3. Update after changes
zappa update production
```

### Pros
- Pay only for requests (very cheap for low traffic)
- Auto-scaling
- No server management

### Cons
- Cold starts (first request may be slow)
- 30 second timeout limit
- Not ideal for heavy database operations

---

## Database Recommendations for Production

### Current: SQLite (Fine for small scale)
- Suitable for: < 100 requests/minute
- Limitations: Single writer, file-based

### Upgrade to: AWS RDS PostgreSQL
```python
# Update DATABASE_URL in environment
DATABASE_URL=postgresql://user:pass@your-rds-endpoint:5432/forecast_db
```

Steps:
1. Create RDS instance in AWS Console
2. Choose PostgreSQL
3. Set DATABASE_URL environment variable
4. Update requirements.txt: add `psycopg2-binary==2.9.9`

---

## Quick Commands Reference

```bash
# Elastic Beanstalk
eb init          # Initialize
eb create        # Create environment
eb deploy        # Deploy updates
eb open          # Open in browser
eb logs          # View logs
eb terminate     # Delete environment

# Check application health
eb health

# SSH into EB instance
eb ssh
```

---

## Data Migration

Since SQLite doesn't work well in production (EB instances are ephemeral):

1. **Option A**: Upload pre-seeded database with deployment
   - Include `forecast.db` in deployment
   - Not recommended for production

2. **Option B**: Use RDS and seed on first deploy
   ```bash
   # SSH into EB instance
   eb ssh
   
   # Run seeder
   cd /var/app/current
   python seed_data.py
   ```

3. **Option C**: Use S3 for database backup
   - Store SQLite backup on S3
   - Download and use on instance start

---

## Monitoring

1. **CloudWatch Logs**: Automatic with EB
2. **CloudWatch Metrics**: CPU, memory, requests
3. **Health Dashboard**: EB Console → Health

---

## Cost Optimization Tips

1. Use **t3.micro** for development/testing (free tier)
2. Use **Reserved Instances** for production (save ~30%)
3. Enable **Auto Scaling** for traffic spikes
4. Use **CloudFront** CDN for static assets
