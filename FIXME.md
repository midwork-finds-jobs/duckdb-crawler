# Known Limitations - JS Variables Extractor

The `js_variables_extractor.cpp` uses a simple string-based parser instead of a full JavaScript AST parser (like tree-sitter). This handles ~95% of real-world cases but has known limitations.

## What Works

Simple JSON assignments that most e-commerce/data sites use:

```javascript
var productData = {"name": "Widget", "price": 19.99};
window.__INITIAL_STATE__ = {"products": [...]};
let config = {"apiUrl": "https://..."};
const APP_DATA = [{"id": 1}, {"id": 2}];
```

## What Doesn't Work

### 1. Regex literals containing `{}`

```javascript
var pattern = /\{[a-z]+\}/;  // Parser sees { and tries to extract JSON
var data = {"valid": true};   // May get confused by earlier regex
```

**Workaround:** None. Regex literals look like division operators to simple parsers.

### 2. Template literals with `${}`

```javascript
const config = `prefix${someVar}suffix`;  // Backtick strings with ${}
var data = {"key": "value"};              // Might misparse
```

**Status:** Partially handled - backtick strings are preserved during comment stripping, but `${}` expressions inside may confuse bracket matching.

### 3. ~~Comments containing JSON-like syntax~~ FIXED

```javascript
// var oldData = {"deleted": true};
var data = {"active": true};
```

**Status:** Fixed. Comments are stripped before parsing.

### 4. ~~JSON.parse with string literals~~ FIXED

```javascript
var data = JSON.parse('[{\x22name\x22:\x22test\x22}]');  // Hex escapes - works!
var data = JSON.parse('{"greeting":"\u0048ello"}');      // Unicode escapes - works!
```

**Status:** Fixed. `JSON.parse('...')` and `JSON.parse("...")` are decoded (hex `\xNN`, unicode `\uNNNN`, and standard escapes).

### 5. Other calculated/dynamic values (impossible without execution)

```javascript
var data = Object.assign({}, defaults);  // Can't resolve
var merged = {...base, ...extra};        // Spread operator
const x = require('./config.json');      // Module import
```

**Workaround:** None without JavaScript execution (would need QuickJS/Boa).

### 6. Destructuring assignments (false positives)

```javascript
const {name, value} = someObject;  // Not a JSON assignment, but looks like one
```

**Status:** Usually rejected by yyjson validation since `{name, value}` isn't valid JSON.

### 7. Multi-declaration statements

```javascript
var a = 1, b = {"key": "value"}, c = 3;  // Only first value checked
```

**Workaround:** None. Parser only looks at first assignment.

### 8. Non-standard JS (minified edge cases)

```javascript
var a={"x":1},b={"y":2}  // No spaces, multiple vars
!function(){var x={"z":3}}()  // IIFE patterns
```

**Status:** May work depending on exact formatting.

## Why Not tree-sitter?

Tree-sitter would solve all parsing issues but:
1. vcpkg's tree-sitter doesn't include JavaScript grammar
2. Grammar files need internal headers not distributed in vcpkg
3. Would add ~3MB to binary size
4. Overkill for extracting simple JSON assignments

## Future Improvements

If these limitations become blocking:
1. **Quick fix:** Add regex literal detection (skip `/...../` patterns)
2. **Medium fix:** Use QuickJS to actually execute and inspect globals
3. **Full fix:** Bundle tree-sitter + tree-sitter-javascript (~3MB)
