#!/bin/zsh

cd /Users/goncalotelesdeabreu/Developer/Lettia_Automation

source lettia_automation/.venv/bin/activate

python - << 'EOF'
from services.sef_form_watcher import SEFFormWatcher

w = SEFFormWatcher()
w.check_for_new_entries()
EOF
