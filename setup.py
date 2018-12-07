from setuptools import setup

setup(name='arcos',
      version='0.1',
      description='Code to process publicly-available'
                  'DEA ARCOS Retail Drug Summary Reports',
      url='http://github.com/akilby/arcos',
      author='Angela E. Kilby',
      author_email='a.kilby@northeastern.edu',
      license='MIT',
      packages=['arcos'],
      install_requires=['setuptools', 'beautifulsoup4', 'requests'],
      zip_safe=False,
      entry_points={
        'console_scripts': [
            'arcos = arcos.core:main',
        ],
      }
      )


# VISION: two commands: "arcos download," "arcos build"

# arcos download --folder /my/path/to/download
