# Steam Import Tool

A Typescript written tool to convert the dataset by Martin Bustos (https://www.kaggle.com/datasets/fronkongames/steam-games-dataset/data) into useable SQL database.

## Getting Started

### Prerequisites

- Node.js v20 or below
- npm

### Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/yourusername/steam-import-tool.git
    cd steam-import-tool
    ```
2. Install dependencies:
    ```bash
    npm install
    ```

### Usage

1. Download the Steam games dataset from [Kaggle](https://www.kaggle.com/datasets/fronkongames/steam-games-dataset/data).
2. Place the dataset file (e.g., `games.csv`) in the project directory.
3. Build the tool:

    ```bash
    npm run build
    ```

4. Run the tool with or without env variables:
    ```bash
    with environment variables (for example):

    ```bash
    INPUT='./my_steam_game.csv' OUTPUT='./my_steam_game.db' npm run start
    ```   ```

### Notes

- Ensure you are using Node.js v20 or below for compatibility.
- The tool converts the CSV dataset into SQL insert statements suitable for database import.

### License

MIT License