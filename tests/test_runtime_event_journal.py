import json

import pytest

from sandbox_runtime.event_journal import ExecutionEventJournal


def test_execution_journal_reloads_unacknowledged_events_and_compacts_segments(tmp_path):
    journal=ExecutionEventJournal(tmp_path,'execution-1',segment_bytes=100,maximum_bytes=4096)
    for sequence in range(1,5):
        journal.append({'kind':'text_delta','payload':{'text':'x'*30},
            'runtime_sequence':sequence})
    assert [row['runtime_sequence'] for row in journal.rows_after(2)]==[3,4]
    assert ExecutionEventJournal.discover(tmp_path)['execution-1'].last_sequence==4

    assert journal.acknowledge(3)==3
    assert json.loads((journal.root/'ack.json').read_text())=={'committed_sequence':3}
    assert journal.last_sequence==4
    assert len(list(journal.root.glob('events-*.ndjson'))) < 4
    assert journal.acknowledge(2)==3
    with pytest.raises(ValueError,match='runtime_ack_sequence_invalid'):
        journal.acknowledge(5)


def test_execution_journal_marks_runtime_restart_as_terminal(tmp_path):
    journal=ExecutionEventJournal(tmp_path,'execution-2')
    journal.append({'kind':'text_delta','payload':{'text':'partial'},'runtime_sequence':1})
    result=journal.mark_interrupted()
    assert result['error_code']=='runtime_process_restarted'
    assert journal.result==result and journal.last_sequence==2
    assert journal.mark_interrupted() is None


def test_execution_journal_repairs_only_a_partial_trailing_record(tmp_path):
    journal=ExecutionEventJournal(tmp_path,'execution-3')
    journal.append({'kind':'text_delta','payload':{'text':'safe'},'runtime_sequence':1})
    segment=next(journal.root.glob('events-*.ndjson'))
    with segment.open('ab') as stream:stream.write(b'{"runtime_sequence":2')
    recovered=ExecutionEventJournal(tmp_path,'execution-3')
    assert recovered.last_sequence==1
    recovered.mark_interrupted()
    assert recovered.result['error_code']=='runtime_process_restarted'


def test_execution_journal_fails_closed_on_a_complete_corrupt_record(tmp_path):
    journal=ExecutionEventJournal(tmp_path,'execution-4')
    journal.append({'kind':'text_delta','payload':{'text':'safe'},'runtime_sequence':1})
    segment=next(journal.root.glob('events-*.ndjson'))
    with segment.open('ab') as stream:stream.write(b'not-json\n')
    with pytest.raises(RuntimeError,match='runtime_journal_record_invalid'):
        ExecutionEventJournal(tmp_path,'execution-4')
