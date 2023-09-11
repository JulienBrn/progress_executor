from setuptools import setup, find_packages


setup(
    name='progress_task_executor',
    packages=find_packages(where='src'),
    package_data={
        "progress_task_executor.package_data": ["*"],
    },
    entry_points={
        'console_scripts': [
            'progress_task_executor = progress_task_executor:run',
        ]
    },
    version='0.1',
    license='MIT',
    description = 'My package description',
    description_file = "README.md",
    author="Julien Braine",
    author_email='julienbraine@yahoo.fr',
    url='https://github.com/JulienBrn/progress_task_executor',
    download_url = 'https://github.com/JulienBrn/progress_task_executor.git',
    package_dir={'': 'src'},
    keywords=['python'],
    install_requires=[],
    #['pandas', 'matplotlib', 'PyQt5', "sklearn", "scikit-learn", "scipy", "numpy", "tqdm", "beautifullogger", "statsmodels", "mat73", "psutil"],
    python_requires=">=3.10"
)
