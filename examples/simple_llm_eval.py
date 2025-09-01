#!/usr/bin/env python3
"""
Simple LLM Evaluation Pipeline Example

A minimal example showing how to use relais for LLM evaluation:
1. Generate test prompts
2. Simulate LLM responses (with delays)
3. Evaluate responses
4. Generate report

Run with: python examples/simple_llm_eval.py
"""

import asyncio
import random
import time
from typing import Any

import relais as r


async def simulate_llm_call(prompt: str) -> dict[str, Any]:
    """Step 1: Simulate calling an LLM API (with realistic delay)."""
    # Simulate network latency + processing time
    delay = random.uniform(0.1, 1.0)
    await asyncio.sleep(delay)

    # Generate mock response based on prompt
    if "math" in prompt.lower():
        response = f"The answer is {random.randint(1, 100)}"
    elif "code" in prompt.lower():
        response = "def solve(): return 42"
    else:
        response = "This is a helpful response."

    return {
        "prompt": prompt,
        "response": response,
        "response_time": delay,
        "tokens": random.randint(20, 100),
    }


async def evaluate_response(llm_result: dict[str, Any]) -> dict[str, Any]:
    """Step 2: Evaluate the LLM response quality."""
    # Simulate evaluation processing
    await asyncio.sleep(random.uniform(0.05, 0.2))

    response = llm_result["response"]

    # Simple scoring based on response content
    if "answer is" in response.lower():
        score = random.uniform(0.8, 1.0)
    elif "def " in response:
        score = random.uniform(0.7, 0.9)
    else:
        score = random.uniform(0.5, 0.8)

    return {
        **llm_result,
        "quality_score": round(score, 2),
        "evaluation_time": time.time(),
    }


def filter_good_responses(result: dict[str, Any]) -> bool:
    """Step 3: Filter out low-quality responses."""
    return result["quality_score"] >= 0.6


async def main():
    """Run the evaluation pipeline."""
    print("🚀 Simple LLM Evaluation Pipeline")
    print("=" * 40)

    # Create test prompts
    prompts = [
        "Solve this math problem: 15 * 7",
        "Write code to reverse a string",
        "Explain photosynthesis",
        "Calculate 23 + 45",
        "Write a function to find max value",
        "What is machine learning?",
        "Solve: 8 * 9 + 12",
        "Code a binary search algorithm",
        "Describe the water cycle",
        "How do neural networks work?",
    ]

    print(f"📝 Testing with {len(prompts)} prompts")
    print("⚡ Running pipeline...")

    start_time = time.time()

    # Build and run pipeline
    results = await (
        prompts
        | r.Map(simulate_llm_call)  # Call LLM for each prompt
        | r.Map(evaluate_response)  # Evaluate each response
        | r.Filter(filter_good_responses)  # Keep only good responses
    ).collect()

    execution_time = time.time() - start_time

    # Show results
    print(f"✅ Completed in {execution_time:.2f} seconds")
    print(f"📊 {len(results)} good responses out of {len(prompts)} total")

    if results:
        avg_score = sum(r["quality_score"] for r in results) / len(results)
        avg_response_time = sum(r["response_time"] for r in results) / len(results)
        total_tokens = sum(r["tokens"] for r in results)

        print(f"📈 Average quality score: {avg_score:.2f}")
        print(f"⏱️  Average response time: {avg_response_time:.2f}s")
        print(f"🔤 Total tokens used: {total_tokens}")

        print("\n🔍 Sample Results:")
        for i, result in enumerate(results[:3]):
            print(f"  {i + 1}. {result['prompt'][:30]}...")
            print(f"     Response: {result['response'][:40]}...")
            print(f"     Score: {result['quality_score']}")
            print()


if __name__ == "__main__":
    asyncio.run(main())
