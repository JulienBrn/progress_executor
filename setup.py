from setuptools import setup, find_packages


setup(
    name='progress_executor',
    packages=find_packages(where='src'),
    package_data={
        "progress_executor.package_data": ["*"],
    },
    entry_points={
        'console_scripts': [
            'progress_executor = progress_executor:run',
        ]
    },
    version='0.1',
    license='MIT',
    description = 'My package description',
    description_file = "README.md",
    author="Julien Braine",
    author_email='julienbraine@yahoo.fr',
    url='https://github.com/JulienBrn/progress_executor',
    download_url = 'https://github.com/JulienBrn/progress_executor.git',
    package_dir={'': 'src'},
    keywords=['python'],
    install_requires=["beautifullogger"],
    python_requires=">=3.11"
)
