"""
AWS Elastic Beanstalk entry point.
EB looks for 'application' variable by default.
"""
from app import create_app

application = create_app('production')

if __name__ == '__main__':
    application.run()
