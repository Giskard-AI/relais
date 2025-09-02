#!/usr/bin/env python3
"""
LLM Evaluation Pipeline Example

This example demonstrates how to use relais for a typical LLM evaluation workflow:
1. Generate diverse user inputs for testing
2. Run the inputs through a target LLM model
3. Evaluate the model responses for quality/correctness
4. Aggregate results and generate a report

This showcases relais' strength in I/O-bound, concurrent processing workflows
with moderate data volumes (hundreds of test cases).
"""

import asyncio
import json
import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List

import relais as r


# Data structures for our evaluation pipeline
class TestCategory(Enum):
    MATH = "math"
    REASONING = "reasoning"
    CODING = "coding"
    GENERAL = "general"


@dataclass
class TestCase:
    """A test case for LLM evaluation."""

    id: str
    category: TestCategory
    prompt: str
    expected_type: str  # "numeric", "code", "explanation", etc.
    difficulty: int  # 1-5 scale


@dataclass
class ModelResponse:
    """Response from the LLM model."""

    test_case: TestCase
    response: str
    response_time_ms: int
    tokens_used: int


@dataclass
class EvaluationResult:
    """Result of evaluating a model response."""

    model_response: ModelResponse
    correctness_score: float  # 0.0-1.0
    quality_score: float  # 0.0-1.0
    evaluation_notes: str


# Mock LLM API (in real usage, this would call OpenAI, Anthropic, etc.)
class MockLLMClient:
    """Mock LLM client that simulates API calls with realistic delays."""

    async def generate(self, prompt: str) -> tuple[str, int, int]:
        """Generate response with simulated network delay and processing time.

        Returns:
            tuple: (response, response_time_ms, tokens_used)
        """
        # Simulate realistic API response times (100-2000ms)
        delay = random.uniform(0.1, 2.0)
        await asyncio.sleep(delay)

        response_time_ms = int(delay * 1000)
        tokens_used = random.randint(50, 500)

        # Generate mock response based on prompt content
        if "calculate" in prompt.lower() or "math" in prompt.lower():
            response = f"The answer is {random.randint(1, 100)}"
        elif "code" in prompt.lower() or "function" in prompt.lower():
            response = "```python\ndef solution():\n    return 42\n```"
        elif "explain" in prompt.lower():
            response = "This is a detailed explanation of the concept..."
        else:
            response = "Based on the information provided, the answer is..."

        return response, response_time_ms, tokens_used


# Mock evaluation functions (in practice, these might use other LLMs or rule-based systems)
class EvaluationEngine:
    """Engine for evaluating LLM responses."""

    @staticmethod
    async def evaluate_response(model_response: ModelResponse) -> EvaluationResult:
        """Evaluate a model response for correctness and quality.

        In a real system, this might:
        - Use another LLM as a judge
        - Apply rule-based checking
        - Compare against known correct answers
        - Check for hallucinations or biases
        """
        # Simulate evaluation processing time
        await asyncio.sleep(random.uniform(0.05, 0.3))

        test_case = model_response.test_case
        response = model_response.response

        # Mock scoring based on test case category
        if test_case.category == TestCategory.MATH:
            correctness = 0.9 if "answer is" in response.lower() else 0.3
            quality = 0.8 if len(response) > 10 else 0.4
            notes = "Mathematical reasoning evaluated"
        elif test_case.category == TestCategory.CODING:
            correctness = 0.85 if "```" in response else 0.2
            quality = 0.9 if "def " in response else 0.5
            notes = "Code structure and syntax evaluated"
        else:
            correctness = random.uniform(0.6, 0.95)
            quality = random.uniform(0.7, 0.9)
            notes = "General response quality evaluated"

        return EvaluationResult(
            model_response=model_response,
            correctness_score=correctness,
            quality_score=quality,
            evaluation_notes=notes,
        )


# Pipeline step functions
async def generate_model_response(test_case: TestCase) -> ModelResponse:
    """Step 1: Generate response from LLM for a test case."""
    client = MockLLMClient()
    response, response_time, tokens = await client.generate(test_case.prompt)

    return ModelResponse(
        test_case=test_case,
        response=response,
        response_time_ms=response_time,
        tokens_used=tokens,
    )


async def evaluate_model_response(model_response: ModelResponse) -> EvaluationResult:
    """Step 2: Evaluate the model response."""
    return await EvaluationEngine.evaluate_response(model_response)


def filter_valid_evaluations(result: EvaluationResult) -> bool:
    """Filter out evaluations that failed or are incomplete."""
    return (
        result.correctness_score >= 0.0
        and result.quality_score >= 0.0
        and len(result.model_response.response) > 5
    )


def create_test_cases() -> List[TestCase]:
    """Generate a set of test cases for evaluation."""
    test_cases = []

    # Math problems
    for i in range(20):
        test_cases.append(
            TestCase(
                id=f"math_{i:03d}",
                category=TestCategory.MATH,
                prompt=f"Calculate the result of {random.randint(10, 100)} * {random.randint(2, 20)}",
                expected_type="numeric",
                difficulty=random.randint(1, 3),
            )
        )

    # Coding problems
    for i in range(15):
        test_cases.append(
            TestCase(
                id=f"code_{i:03d}",
                category=TestCategory.CODING,
                prompt=f"Write a Python function that sorts a list of {random.choice(['integers', 'strings', 'tuples'])}",
                expected_type="code",
                difficulty=random.randint(2, 4),
            )
        )

    # Reasoning problems
    for i in range(25):
        topics = ["climate change", "economics", "philosophy", "science"]
        test_cases.append(
            TestCase(
                id=f"reason_{i:03d}",
                category=TestCategory.REASONING,
                prompt=f"Explain the relationship between {random.choice(topics)} and human society",
                expected_type="explanation",
                difficulty=random.randint(3, 5),
            )
        )

    return test_cases


async def generate_evaluation_report(results: List[EvaluationResult]) -> Dict[str, Any]:
    """Generate final evaluation report from results."""
    if not results:
        return {"error": "No results to process"}

    # Calculate aggregate metrics
    total_tests = len(results)
    avg_correctness = sum(r.correctness_score for r in results) / total_tests
    avg_quality = sum(r.quality_score for r in results) / total_tests
    avg_tokens = sum(r.model_response.tokens_used for r in results) / total_tests
    avg_response_time = (
        sum(r.model_response.response_time_ms for r in results) / total_tests
    )

    # Breakdown by category
    category_stats = {}
    for category in TestCategory:
        category_results = [
            r for r in results if r.model_response.test_case.category == category
        ]
        if category_results:
            category_stats[category.value] = {
                "count": len(category_results),
                "avg_correctness": sum(r.correctness_score for r in category_results)
                / len(category_results),
                "avg_quality": sum(r.quality_score for r in category_results)
                / len(category_results),
            }

    # Performance metrics
    performance_stats = {
        "total_tokens_used": sum(r.model_response.tokens_used for r in results),
        "total_response_time_ms": sum(
            r.model_response.response_time_ms for r in results
        ),
        "avg_tokens_per_response": avg_tokens,
        "avg_response_time_ms": avg_response_time,
    }

    return {
        "summary": {
            "total_tests": total_tests,
            "average_correctness_score": round(avg_correctness, 3),
            "average_quality_score": round(avg_quality, 3),
            "overall_score": round((avg_correctness + avg_quality) / 2, 3),
        },
        "category_breakdown": category_stats,
        "performance": performance_stats,
        "timestamp": time.time(),
    }


async def main():
    """Main evaluation pipeline execution."""
    print("🚀 Starting LLM Evaluation Pipeline")
    print("=" * 50)

    # Generate test cases
    test_cases = create_test_cases()
    print(f"📝 Generated {len(test_cases)} test cases")

    # Create and run the evaluation pipeline
    print("⚡ Running concurrent evaluation pipeline...")
    start_time = time.time()

    try:
        # Build the pipeline using relais
        evaluation_pipeline = (
            test_cases
            | r.Map(generate_model_response)  # Generate LLM responses concurrently
            | r.Map(evaluate_model_response)  # Evaluate responses concurrently
            | r.Filter(filter_valid_evaluations)  # Filter out failed evaluations
        )

        # Execute pipeline and collect results
        results = await evaluation_pipeline.collect()

        # Generate final report
        report = await generate_evaluation_report(results)

        execution_time = time.time() - start_time
        print(f"✅ Pipeline completed in {execution_time:.2f} seconds")
        print(f"📊 Processed {len(results)} successful evaluations")

        # Display results
        print("\n" + "=" * 50)
        print("📈 EVALUATION REPORT")
        print("=" * 50)
        print(json.dumps(report, indent=2))

        # Show some example results
        print("\n" + "=" * 50)
        print("🔍 SAMPLE EVALUATIONS")
        print("=" * 50)
        for i, result in enumerate(results[:3]):  # Show first 3 results
            tc = result.model_response.test_case
            print(f"\nExample {i + 1} [{tc.category.value}]:")
            print(f"  Prompt: {tc.prompt[:60]}...")
            print(f"  Response: {result.model_response.response[:80]}...")
            print(f"  Correctness: {result.correctness_score:.2f}")
            print(f"  Quality: {result.quality_score:.2f}")
            print(f"  Response Time: {result.model_response.response_time_ms}ms")

    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    # Run the evaluation pipeline
    asyncio.run(main())
