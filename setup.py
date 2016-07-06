from distutils.core import setup

setup(
    name='opencluster',
    version='1.0',
    packages=['opencluster'],
    package_dir={'opencluster': 'opencluster'},
    url='',
    license='',
    author='Shoulin Wei',
    author_email='wsl@cnlab.net',
    requires=['psutil','Pyro4','web.py'],
    description=''
)
