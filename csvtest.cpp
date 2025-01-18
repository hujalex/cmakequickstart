#include <iostream>
#include <fstream>
#include <vector>
#include <iomanip>

void writeCSV() {
    std::ofstream outFile("test.csv");
    outFile << std::fixed << std::setprecision(4);  // Set precision for pi

    // Write 1000 rows
    for (int row = 0; row < 1000; row++) {
        // Write 10 columns
        for (int col = 0; col < 10; col++) {
            outFile << 3.1415f;
            if (col < 9) outFile << ",";
        }
        outFile << "\n";
    }
    outFile.close();
}

void readCSV() {
    std::ifstream inFile("test.csv");
    std::string line;
    int count = 0;
    
    while (std::getline(inFile, line)) {
        count++;
    }
    std::cout << "Read " << count << " lines from CSV" << std::endl;
    inFile.close();
}

int main() {
    writeCSV();
    // readCSV();
    return 0;
}