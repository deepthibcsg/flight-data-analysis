# Flight Analytics Project

## Table of Contents
- [Introduction](#introduction)
- [Installation & Setup](#installation--setup)
- [Usage](#usage)
- [Running Tests](#running-tests)
- [Dataset Description](#dataset-description)
- [Assumptions & Design Decisions](#assumptions--design-decisions)
- [Contributing & Feedback](#contributing--feedback)
- [License](#license)

## Introduction
This project is meant to analyze two datasets: `flightData.csv`, which contains flight information, and `passengers.csv`, which contains passenger details. The program extracts insights such as the most frequent flyers, the longest travel streaks without visiting the UK, and more.

## Installation & Setup
1. **Clone this repository:**
   ```bash
   git clone https://github.com/deepthibcsg/flight-data-analysis.git
   ```
2. **Navigate to the project directory:**
   ```bash
   cd FlightAnalytics
   ```
3. **Ensure you have SBT (Scala Build Tool) installed.**
4. **Build the project:**
   ```bash
   sbt compile
   ```
## Usage
To run the main application:
   ```bash
   sbt run
   ```
   
Results will be printed in the console. Alternatively, they can be saved to a CSV or other formats if implemented.

## Running Tests
To ensure the correctness and performance of the code, run:
   ```bash
   sbt test
   ```

## Dataset Description
- `flightData.csv`:
  - `passengerId`: Integer representing the id of a passenger.
  - `flightId`: Integer representing the id of a flight.
  - `from`: String representing the departure country.
  - `to`: String representing the destination country.
  - `date`: String representing the date of a flight in the format `YYYY-MM-DD`.

- `passengers.csv`:
  - `passengerId`: Integer representing the id of a passenger.
  - `firstName`: String representing the first name of a passenger.
  - `lastName`: String representing the last name of a passenger.

## Assumptions & Design Decisions
- The date format in the dataset is consistent and is `YYYY-MM-DD`.
- No two flights have the same `flightId`.
- Flights are unique to each passenger on a given date (i.e., a passenger cannot travel to two places on the same day).
- For the purpose of this exercise, the data size is assumed to fit into memory, but the design caters to scalability considerations for larger datasets.

## Contributing & Feedback
If you have suggestions or find bugs, please open an issue with detailed information and steps to reproduce.

## License
MIT License. See LICENSE for more information.

