"""Scheduler and run Jobs"""

import live_carbon_intensity
import historical_carbon_intensity

import pandas as pd
import time
import datetime
import uuid

from loguru import logger


class Scheduler():
    """Scheduler object, will check if conditions are met and run jobs"""
    def __init__(self):
        self.jobs = []
        self.carbon_intensity = {}

    def add_job(self, func, args=[], kwargs={}):
        """Add a job in the queue"""
        job = Job(func, args, kwargs)
        self.jobs.append(job)
        logger.debug(f'Job {job} added to the queue')
        return job
    
    def _update_carbon_intensity(self):
        """Update values in internal dict of region:carbon_intensity"""
        for region in self.carbon_intensity:
            try:
                value = live_carbon_intensity.get_carbon_intensity_by_country_code(region)
            except:
                logger.error(f'Error in updating carbon intensity for {region}')
                value = None
            self.carbon_intensity[region] = value
         
    def _preconfig(self):
        """Checks and parameters initialization before running the scheduler"""
        for job in self.jobs:          
            if job.has_missing_information():
                logger.warning(f'Job {job} is missing some informations, it will be removed.')
                self.jobs.remove(job)
                continue
            job.update_execution_dates()
            self.carbon_intensity[job.region] = None
            
    def run(self, frequency="15min"):
        """Run the scheduler"""
        self._preconfig()
        while True:
            self._update_carbon_intensity()
            logger.debug(f'Updated carbon intensities :{self.carbon_intensity}')
            for job in self.jobs:
                if job.is_runnable(self.carbon_intensity[job.region]['value']):
                    job.run()
                else:
                    logger.debug('job is not runnable yet')
            logger.info(f'{len(self.jobs)} jobs still in queue')
            time.sleep(pd.to_timedelta(frequency).total_seconds())
     
    @property
    def jobs_descriptions(self):
        """Return jobs details"""
        return [job.description for job in self.jobs]
    
    def __repr__(self):
        return '\n'.join(self.jobs_descriptions)
    

class Job():
    """A job to schedule"""
    def __init__(self, func, args=[], kwargs={}):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.uuid = uuid.uuid4()
        
    def every(self, frequency):
        """Add the time component - Mandatory"""
        self.frequency = frequency
        return self
        
    def when_carbon_impact_in(self, region):
        """Add the location component - Mandatory"""
        self.region = region
        return self
        
    def is_lt(self, threshold):
        """Add the Carbon Threshold component - You can also use the .in_quantile method"""
        if getattr(self, 'threshold', False):
            raise(ValueError('Carbon threshold is already defined, please use `is_lt` or `in_quantile` but not both'))
        self.threshold = threshold
        return self
        
    def in_quantile(self, quantile):
        """Add the Carbon Threshold component - You can also use the .is_lt method"""
        if getattr(self, 'threshold', False):
            raise(ValueError('Carbon threshold is already defined, please use `is_lt` or `in_quantile` but not both'))
        if not getattr(self, 'region', False):
            raise(ValueError('Please defined `region` using method `.every` before using `.in_quantile`'))
        self.threshold = historical_carbon_intensity.carbon_impact_threshold(self.region, '1w', quantile)  # TODO(jeremie): change that with prediction model
        return self
        
    def update_execution_dates(self):
        """Setup window during which the script should be executed"""
        start = datetime.datetime.now()
        if getattr(self, "latest_next_execution", False):
            start = self.latest_next_execution
            
        self.soonest_next_execution = start
        self.latest_next_execution = start + pd.to_timedelta(self.frequency)
        
        logger.debug(f'job to be executed between {self.soonest_next_execution} and {self.latest_next_execution}')
        
    def is_runnable(self, carbon_intensity):
        """Tell if a job should be run now"""
        now = datetime.datetime.now()
        if now > self.latest_next_execution:
            logger.debug(f'Latest next executing is passed: Job is runnable')
            return True
        if now < self.soonest_next_execution:
            return False
        if carbon_intensity <= self.threshold:
            logger.debug(f'Carbon intensity is OK!')
        return carbon_intensity <= self.threshold
    
    def has_missing_information(self):
        """Check if all necessary information are set"""
        missing = False
        if not getattr(self, 'frequency', False):
            logger.warning(f'Job {self} is missing `frequency`, use `.every` method')
            missing = True
        if not getattr(self, 'region', False):
            logger.warning(f'Job {self} is missing `region`, use `.when_carbon_impact_in` method')
            missing = True
        if not getattr(self, 'threshold', False):
            logger.warning(f'Job {self} is missing `threshold`, use `.is_lt` or `in_quantile` method')
            missing = True
        return missing  
        
    def run(self):
        """Execute the job"""
        logger.info(f'Running: {self}')
        self.func(*self.args, **self.kwargs)
        self.update_execution_dates()
        
    def __repr__(self):
        return f'{self.uuid}'
    
    @property
    def description(self):
        return (f'Job {self.uuid} scheduled in region:{getattr(self, "region", None)} '
                f'every:{getattr(self, "frequency", None)} '
                f'with threshold:{getattr(self, "threshold", None)} '
                f'running function:{getattr(self.func, "__name__", self.func)}'
        )
    