from setuptools import find_packages, setup

setup(name='arcos',
      python_requires='>=3.3',
      version='0.1',
      description='Code to process publicly-available'
                  'DEA ARCOS Retail Drug Summary Reports',
      url='http://github.com/akilby/arcos',
      author='Angela E. Kilby',
      author_email='a.kilby@northeastern.edu',
      license='MIT',
      packages=find_packages('src'),
      include_package_data=True,
      package_dir={'': 'src'},
      install_requires=['setuptools', 'beautifulsoup4',
                        'requests', 'pdfminer.six', 'pandas'],
      zip_safe=False,
      entry_points={
        'console_scripts': [
            'arcos = arcos.__main__:main',
        ],
      }
      )
