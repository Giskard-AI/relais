"""Test script for error handling functionality."""
import asyncio
import relais as r

async def test_fail_fast():
    """Test fail-fast error policy."""
    print("Testing fail-fast error policy...")
    
    def failing_function(x):
        if x == 2:
            raise ValueError(f"Intentional error for x={x}")
        return x * 2
    
    pipeline = r.Pipeline([r.map(failing_function)], error_policy=r.ErrorPolicy.FAIL_FAST)
    
    try:
        result = await pipeline.collect([1, 2, 3, 4])
        print(f"ERROR: Should have failed but got: {result}")
    except r.PipelineError as e:
        print(f"✓ Correctly caught error: {e}")
    except Exception as e:
        print(f"✓ Got expected error type: {type(e).__name__}: {e}")

async def test_ignore_errors():
    """Test ignore error policy."""
    print("\nTesting ignore error policy...")
    
    def failing_function(x):
        if x == 2:
            raise ValueError(f"Intentional error for x={x}")
        return x * 2
    
    pipeline = r.Pipeline([r.map(failing_function)], error_policy=r.ErrorPolicy.IGNORE)
    
    try:
        result = await pipeline.collect([1, 2, 3, 4])
        print(f"✓ Ignore policy result (should skip x=2): {sorted(result)}")
        expected = [2, 6, 8]  # x=1->2, x=3->6, x=4->8, x=2 skipped
        if sorted(result) == sorted(expected):
            print("✓ Correct items processed, error item ignored")
        else:
            print(f"✗ Expected {expected}, got {result}")
    except Exception as e:
        print(f"✗ Should not have raised error: {e}")

async def test_collect_errors():
    """Test collect error policy."""  
    print("\nTesting collect error policy...")
    
    def failing_function(x):
        if x == 2 or x == 4:  
            raise ValueError(f"Intentional error for x={x}")
        return x * 2
    
    pipeline = r.Pipeline([r.map(failing_function)], error_policy=r.ErrorPolicy.COLLECT)
    
    try:
        result = await pipeline.collect([1, 2, 3, 4])
        print(f"✓ Collect policy result: {sorted(result)}")
        expected = [2, 6]  # x=1->2, x=3->6, x=2 and x=4 failed
        if sorted(result) == sorted(expected):
            print("✓ Correct items processed")
        else:
            print(f"Expected {expected}, got {result}")
    except Exception as e:
        print(f"Collected errors, but got exception: {e}")

async def main():
    await test_fail_fast()
    await test_ignore_errors() 
    await test_collect_errors()

if __name__ == "__main__":
    asyncio.run(main())