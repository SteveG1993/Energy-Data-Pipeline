import time
import random
from typing import List, Set

def find_duplicates_quadratic(data: List[int]) -> List[int]:
    """
    Find duplicate values in a list using nested loops.
    Time Complexity: O(n²) - Quadratic
    Space Complexity: O(k) where k is number of duplicates
    
    This approach compares every element with every other element.
    """
    duplicates = []
    n = len(data)
    
    for i in range(n):
        for j in range(i + 1, n):
            # Compare each element with all subsequent elements
            if data[i] == data[j] and data[i] not in duplicates:
                duplicates.append(data[i])
    
    return duplicates


def find_duplicates_linear(data: List[int]) -> List[int]:
    """
    Find duplicate values in a list using a set for tracking.
    Time Complexity: O(n) - Linear
    Space Complexity: O(n) - trades space for time
    
    This approach uses a hash set to track seen elements in single pass.
    """
    seen = set()
    duplicates = set()
    
    for item in data:
        if item in seen:
            duplicates.add(item)
        else:
            seen.add(item)
    
    return list(duplicates)


def find_duplicates_optimized(data: List[int]) -> Set[int]:
    """
    Most optimized version using set comprehensions and collections.Counter
    Time Complexity: O(n) - Linear
    Space Complexity: O(n)
    
    This is the most Pythonic approach for production data pipelines.
    """
    from collections import Counter
    counts = Counter(data)
    return {item for item, count in counts.items() if count > 1}


def benchmark_functions(data_sizes: List[int]) -> None:
    """
    Benchmark the different approaches with varying data sizes.
    This demonstrates the practical impact of algorithm choice.
    """
    print("=" * 70)
    print("DUPLICATE FINDING ALGORITHM COMPARISON")
    print("=" * 70)
    print(f"{'Size':<10} {'Quadratic (s)':<15} {'Linear (s)':<15} {'Speedup':<10}")
    print("-" * 70)
    
    for size in data_sizes:
        # Generate test data with some duplicates
        data = [random.randint(1, size // 2) for _ in range(size)]
        
        # Benchmark quadratic approach
        start_time = time.time()
        result_quad = find_duplicates_quadratic(data)
        quadratic_time = time.time() - start_time
        
        # Benchmark linear approach
        start_time = time.time()
        result_linear = find_duplicates_linear(data)
        linear_time = time.time() - start_time
        
        # Verify results are equivalent (convert to sets for comparison)
        assert set(result_quad) == set(result_linear), "Results don't match!"
        
        # Calculate speedup
        speedup = quadratic_time / linear_time if linear_time > 0 else float('inf')
        
        print(f"{size:<10} {quadratic_time:<15.4f} {linear_time:<15.4f} {speedup:<10.1f}x")


def data_engineering_example():
    """
    Real-world example: cleaning customer data for duplicates
    """
    print("\n" + "=" * 70)
    print("DATA ENGINEERING SCENARIO: Customer Data Deduplication")
    print("=" * 70)
    
    # Simulate customer IDs from different data sources
    customer_ids = [
        101, 102, 103, 101,  # Source 1: CRM system
        104, 105, 102, 106,  # Source 2: Email system  
        107, 103, 108, 105,  # Source 3: Transaction system
        109, 110, 101, 107   # Source 4: Support system
    ]
    
    print(f"Original customer IDs: {customer_ids}")
    print(f"Total records: {len(customer_ids)}")
    
    # Find duplicates using optimized method
    duplicates = find_duplicates_optimized(customer_ids)
    print(f"Duplicate customer IDs found: {sorted(duplicates)}")
    
    # Clean the data (remove duplicates)
    clean_ids = list(set(customer_ids))
    print(f"Clean customer IDs: {sorted(clean_ids)}")
    print(f"Records after deduplication: {len(clean_ids)}")
    print(f"Duplicate records removed: {len(customer_ids) - len(clean_ids)}")


def complexity_analysis():
    """
    Explain the complexity differences with mathematical analysis
    """
    print("\n" + "=" * 70)
    print("COMPLEXITY ANALYSIS")
    print("=" * 70)
    
    analysis = """
    QUADRATIC O(n²) APPROACH:
    • Nested loops: outer loop runs n times, inner loop runs n-1, n-2, ... 1 times
    • Total comparisons: n×(n-1)/2 ≈ n²/2
    • For 1,000 records: ~500,000 comparisons
    • For 10,000 records: ~50,000,000 comparisons
    • Growth rate: 100x more data = 10,000x more time
    
    LINEAR O(n) APPROACH:
    • Single loop: each element processed exactly once
    • Hash set lookups: O(1) average case
    • Total operations: n (one per element)
    • For 1,000 records: ~1,000 operations
    • For 10,000 records: ~10,000 operations
    • Growth rate: 100x more data = 100x more time
    
    PRACTICAL IMPLICATIONS FOR DATA ENGINEERING:
    • ETL pipelines: Linear algorithms scale with data volume
    • Real-time processing: Quadratic algorithms cause timeouts
    • Memory vs Time: Linear often trades space for time efficiency
    • Database operations: Understanding complexity helps with query optimization
    """
    
    print(analysis)


if __name__ == "__main__":
    # Run the data engineering example
    data_engineering_example()
    
    # Show complexity analysis
    complexity_analysis()
    
    # Benchmark with increasing data sizes
    print("\nRunning performance benchmarks...")
    # Start with smaller sizes to avoid long wait times
    test_sizes = [100, 500, 1000, 2000]
    benchmark_functions(test_sizes)
    
    print("\n" + "=" * 70)
    print("KEY TAKEAWAYS FOR DATA ENGINEERING:")
    print("=" * 70)
    print("1. Always consider algorithm complexity when processing large datasets")
    print("2. Hash-based approaches (sets, dicts) often provide O(1) lookups")
    print("3. Trading memory for time is usually worthwhile in data pipelines")
    print("4. Use built-in Python collections (Counter, set) for optimization")
    print("5. Profile your code with realistic data volumes during development")