# Foreca FTP upload

> Periodically upload measurements to Foreca.

## Setup

```bash
cp .env.example .env
chmod 600 .env
# > Edit .env

sudo cp ./cronjob /etc/cron.d/foreca-ftp-upload
sudo service cron reload

# Upload data for 10 days
docker-compose run --rm upload npm start '' 864000
```

## License

This software is licensed under the MIT license..

© 2016 Kukua BV
