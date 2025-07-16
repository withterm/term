# Diátaxis Quick Reference

## The Four Quadrants at a Glance

```
WHEN LEARNING                    WHEN WORKING
     ↓                                ↓
┌─────────────────────┬─────────────────────┐
│                     │                     │
│    📚 TUTORIALS     │   🔧 HOW-TO GUIDES  │ ← PRACTICAL
│                     │                     │   (hands-on)
│  "Learn by doing"   │  "Get things done"  │
│                     │                     │
├─────────────────────┼─────────────────────┤
│                     │                     │
│  💡 EXPLANATION     │   📖 REFERENCE      │ ← THEORETICAL
│                     │                     │   (knowledge)
│ "Understand why"    │   "Look things up"  │
│                     │                     │
└─────────────────────┴─────────────────────┘
     ↑                                ↑
UNDERSTANDING ORIENTED      INFORMATION ORIENTED
```

## Quick Decision Tree

```
Is the reader trying to...
│
├─ Learn Term for the first time?
│  └─ ✅ TUTORIAL
│
├─ Accomplish a specific task?
│  └─ ✅ HOW-TO GUIDE
│
├─ Look up technical details?
│  └─ ✅ REFERENCE
│
└─ Understand concepts/design?
   └─ ✅ EXPLANATION
```

## What Goes Where?

### 📚 TUTORIALS
- First-time user experiences
- Step-by-step learning paths
- Exercises and experiments
- Building mental models
- "Hello World" examples

**Example titles:**
- "Getting Started with Term"
- "Your First Data Validation"
- "Learning Constraints Step by Step"

### 🔧 HOW-TO GUIDES
- Specific task completion
- Problem-solving recipes
- Best practices
- Integration guides
- Troubleshooting

**Example titles:**
- "How to Validate CSV Files"
- "How to Optimize Performance"
- "How to Migrate from Deequ"

### 📖 REFERENCE
- API documentation
- Configuration options
- Parameter lists
- Error codes
- CLI commands

**Example titles:**
- "Constraint API Reference"
- "Configuration Options"
- "Error Code Reference"

### 💡 EXPLANATION
- Architecture overviews
- Design decisions
- Conceptual models
- Comparisons
- Background theory

**Example titles:**
- "Understanding Constraints"
- "Term vs Deequ: Design Philosophy"
- "How Validation Works Internally"

## Writing Rules

### ✅ DO
- Keep each document in ONE quadrant
- Link between quadrants
- Use templates consistently
- Match reader's goal to quadrant

### ❌ DON'T
- Mix learning with doing
- Explain in reference docs
- Add tasks to explanations
- Create comprehensive tutorials

## Language Patterns

### Tutorials
- "Let's..." "We'll..." "You'll learn..."
- Present tense, active voice
- Encouraging, supportive tone
- Acknowledge mistakes are normal

### How-To Guides
- "To [task], follow these steps..."
- Imperative mood
- Direct, efficient language
- Results-focused

### Reference
- "This function..." "Parameters include..."
- Descriptive, precise language
- Complete technical accuracy
- No opinions or recommendations

### Explanation
- "The reason..." "This works because..."
- Explanatory, discursive tone
- Connect ideas and concepts
- Discuss trade-offs

## Common Pitfalls

| Mistake | Why It Happens | Fix |
|---------|----------------|-----|
| Tutorial explains too much | Trying to be complete | Move theory to Explanation |
| How-To teaches concepts | Assuming no knowledge | Move learning to Tutorial |
| Reference includes examples | Trying to be helpful | Move tasks to How-To |
| Explanation gives instructions | Trying to be practical | Move steps to How-To |

## Quick Test

Before publishing, ask:

1. **Single Purpose?** Does it serve exactly one goal?
2. **Right Quadrant?** Does it match the reader's intent?
3. **Appropriate Depth?** Not too much, not too little?
4. **Proper Links?** Does it reference other quadrants?
5. **Correct Tone?** Does the language fit the type?

## File Naming Conventions

```
docs/
├── tutorials/
│   └── 01-getting-started.md      # Numbered for sequence
├── how-to/
│   └── validate-csv-files.md      # Task-focused verb phrases
├── reference/
│   └── constraint-api.md          # Noun-based names
└── explanation/
    └── architecture-overview.md   # Concept-focused titles
```

## Remember

> "The secret of the Diátaxis approach is that it solves problems for both readers and writers. It makes it easier to find the right information, and easier to put information in the right place."

Each quadrant serves a different need at a different time. Respect the boundaries!