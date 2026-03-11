# Saga Pattern

A way to manage transactions across multiple microservices

## The Problem

In a monolith, ACID transactions work fine - either everything saves or nothing does. In microservices, each service has its own database, so there's no shared transaction

## How It Works

Break one big transaction into a chain of small local ones. If something fails - run compensating transactions to undo previous steps


## Two Types

Choreography - services talk to each other via events, no central controller. Simple but hard to debug

Orchestration - one central Saga Manager tells each service what to do. Easier to track but single point of failure

## Compensating Transactions

Not a rollback - it`s business logic to undo an action

## When to Use

Microservices with long business flows (orders, bookings, payments)