# Order Book Implementation using Adaptive Radix Tree (ART)

This project implements an order book using the Adaptive Radix Tree (ART) data structure. The order book is designed to efficiently handle large volumes of orders, providing high-performance insertion, search, and deletion operations.

## Features

- **Price Priority**: Orders are prioritized based on price, with higher buy orders and lower sell orders being matched first.
- **Efficient Insertion**: Supports efficient insertion of new orders into the order book.
- **Efficient Deletion**: Supports efficient deletion of existing orders from the order book.
- **Order Matching**: Efficiently matches buy and sell orders based on price and quantity.
- **High-Performance Data Structure**: Utilizes the Adaptive Radix Tree (ART) for high-performance order management.

## Installation

To use this project, you need to have Java installed on your system. You can clone the repository and build the project using Maven.

```sh
# Clone the repository
git clone https://github.com/yourusername/order-book-art.git

# Navigate to the project directory
cd order-book-art

# Build the project using Maven
mvn clean install
