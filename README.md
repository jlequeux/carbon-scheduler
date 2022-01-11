# Carbon Scheduler

## Why
Information and Communications Technology's (ICT) current share of global greenhouse gas (GHG) emissions is estimated between [1.8 and 2.8%](https://arxiv.org/abs/2102.02622) of global GHG emissions

It also represent about ~10% of all the global energy consumption and could grow up to [20% by 2030](https://www.nature.com/articles/d41586-018-06610-y)

**With this information, it has become clear that we need to optimize our energy use and carbon footprint in the software engineering world.**

This is already well documented in the [Principles of Green Software Engineering](https://principles.green/)

One of the solutions described in this manifest to reduce carbon footprint from our software is to use energy when the [carbon intensity](https://principles.green/principles/carbon-intensity/) is the lowest


## What is carbon-scheduler
This project is here to explore solutions to reduce carbon impact of software engineering.

`carbon-scheduler` helps you to schedule your scripts when the [carbon intensity](https://principles.green/principles/carbon-intensity/) is the lowest.
It is using [Co2Signal API](https://api.co2signal.com) to get the latest carbon intensity at the location of your datacenter and schedule your scripts when the value is under a defined threshold or if the time to schedule is over.


## Usage
```python3
import carbon_scheduler

def func(message):
    print(message)
    
scheduler = carbon_scheduler.Scheduler()
scheduler.add_job(func, ['hello its been 15min']).every('15min').when_carbon_impact_in('FR').is_lt(23)
scheduler.add_job(func, ['hello its been 2days']).every('2d').when_carbon_impact_in('DE').in_quantile(0.2)

scheduler.run()

```

## Disclamer and contact
This is a work in progress project.
If you are interested in helping or you just want to connect, please [contact me](https://www.linkedin.com/in/j%C3%A9r%C3%A9mie-lequeux/)
