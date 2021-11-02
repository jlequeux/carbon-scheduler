import setuptools

setuptools.setup(
   name='carbon_scheduler',
   version='1.0',
   author='jlequeux',
   author_email='jlequeux@gmail.com',
   packages=setuptools.find_packages(),
   license='LICENSE',
   description='Schedule your script when the carbon impact is the lowest',
   long_description=open('README.md').read(),
)