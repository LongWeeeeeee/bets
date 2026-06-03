#!/usr/bin/env bash
input=$(cat)
P='AGENTS\.md|CLAUDE\.md|GEMINI\.md|\.cursorrules|copilot-instructions\.md'
py='import sys,json;d=json.load(sys.stdin)'
tool=$(printf '%s' "$input" | python3 -c "$py;print(d.get('tool_name',''))")
if [ "$tool" = "Bash" ]; then
  field=$(printf '%s' "$input" | python3 -c "$py;print(d.get('tool_input',{}).get('command',''))")
else
  field=$(printf '%s' "$input" | python3 -c "$py;print(d.get('tool_input',{}).get('file_path',''))")
fi
if printf '%s' "$field" | grep -Eq "($P)"; then
  echo "BLOCKED: AGENTS.md и его симлинки меняет только человек вручную." >&2
  exit 2
fi
exit 0
