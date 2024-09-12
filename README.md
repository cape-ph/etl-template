# CAPE ETL Script Template

This provides a template for easily getting started creating ETL scripts for the
CAPE system. The script provides the boilerplate for retrieving the raw data
file passed into the transform as well as the uploading of the cleaned data once
it is created.

## Usage

1. Click the "Use this template" button at the top of the repository which will
   guide you through creating a new repository from this template code.
2. Update the `README.md` to describe the transformation
3. Update the `main.py` file to make the necessary modifications as indicated by
   the "TODO" comments in the code:

    - Make necessary transformations to the data made available in the `raw`
      variable and store it into the `cleaned` variable
    - Update the `cleaned_key` variable to have the correct final filename for
      the cleaned file

4. Add any necessary extra dependencies to the `requirements.txt` file

## Releasing

Once the code is at a good point to start being used in the CAPE system we will
need releases to be tagged and maintained. We have workflows built into the
repository through GitHub Actions to help make this as easy as possible.

If you navigate to the "Actions" tab at the top of the repository and go to the
"Release" action, there is a button to "Run workflow". If you click this and run
the workflow then the script will calculate the appropriate version, create a
new tag, and create a new release in GitHub.

> [!NOTE]
> By default this is set up to follow a date based versioning schema. This is in
> the format of `YYYY.MM.DD`. If multiple versions are tagged on the same day
> then a revision number will be appended to the end such as `YYYY.MM.DD.1`.

## Best Practices

This repository comes bundled with the recommended CAPE practices such as type
checking and format checking. It is recommended to use the following tools to
make sure the tests pass:

-   Use `pyright` language server or the Python language support in VS Code to
    properly check the types and validity of the code
-   Use `black` and `isort` to format the code

> [!TIP]
> The repository comes with the necessary configuration for VS Code built in.
> When loading the project it may pop up to install recommended extensions. If
> you install all of them then the editor will configure itself to follow the
> above practices.
