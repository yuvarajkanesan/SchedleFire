"""Applies state transitions: runs election/apply filters, sets expiration, and
triggers continuations when a parent job finishes."""

from __future__ import annotations

import logging
import time
from typing import Optional, Sequence

from .filters import ApplyStateContext, ElectStateContext, FilterCollection, filters_for_job
from .job import Job
from .states import (
    DELETED,
    SUCCEEDED,
    AwaitingState,
    ContinuationOptions,
    DeletedState,
    EnqueuedState,
    State,
)
from .storage.base import JobStorage

logger = logging.getLogger("schedulefire")


class StateMachine:
    def __init__(self, storage: JobStorage, global_filters: Optional[FilterCollection] = None):
        self.storage = storage
        self.global_filters = global_filters

    def change_state(
        self,
        job_id: str,
        state: State,
        *,
        expected_states: Optional[Sequence[Optional[str]]] = None,
    ) -> bool:
        """Move a job to ``state``. Returns False if the job is missing or not in ``expected_states``."""
        record = self.storage.get_job(job_id)
        if record is None:
            return False
        if expected_states is not None and record.state_name not in expected_states:
            return False

        job: Optional[Job] = record.job
        filters = filters_for_job(job, self.global_filters)

        if isinstance(state, EnqueuedState) and state.queue is None:
            state.queue = record.queue
        election = ElectStateContext(self.storage, job_id, job, record.state_name, state)
        for f in filters:
            f.on_state_election(election)
        state = election.candidate_state
        if isinstance(state, EnqueuedState) and state.queue is None:
            state.queue = record.queue

        if isinstance(state, AwaitingState):
            # Register before the state is visible, so a parent finishing concurrently
            # either sees this continuation or we see the parent's final state below.
            self.storage.add_continuation(state.parent_id, job_id)

        expire_at = time.time() + self.storage.job_expiration if state.is_final else None
        if not self.storage.set_state(job_id, state, expected_states=expected_states, expire_at=expire_at):
            return False

        applied = ApplyStateContext(self.storage, job_id, job, record.state_name, state)
        for f in filters:
            f.on_state_applied(applied)

        if isinstance(state, AwaitingState):
            parent = self.storage.get_job(state.parent_id)
            next_state = (
                DeletedState(reason=f"Parent job #{state.parent_id} does not exist.")
                if parent is None
                else self._continuation_state(state.parent_id, parent.state_name, state.options)
            )
            if next_state is not None:
                self.change_state(job_id, next_state, expected_states=[AwaitingState.name])
        elif state.name in (SUCCEEDED, DELETED):
            self._run_continuations(job_id, state.name)
        return True

    @staticmethod
    def _continuation_state(parent_id: str, parent_state: Optional[str], options) -> Optional[State]:
        if parent_state == SUCCEEDED or (
            parent_state == DELETED and options == ContinuationOptions.ON_ANY_FINISHED_STATE
        ):
            return EnqueuedState(reason=f"Continuation of job #{parent_id}")
        if parent_state == DELETED:
            return DeletedState(reason=f"Parent job #{parent_id} was deleted.")
        return None

    def _run_continuations(self, parent_id: str, parent_state: str) -> None:
        for child_id in self.storage.get_continuations(parent_id):
            try:
                child = self.storage.get_job(child_id)
                if child is None or child.state_name != AwaitingState.name:
                    continue
                options = child.state_data.get("options", ContinuationOptions.ONLY_ON_SUCCEEDED_STATE)
                next_state = self._continuation_state(parent_id, parent_state, options)
                if next_state is not None:
                    self.change_state(child_id, next_state, expected_states=[AwaitingState.name])
            except Exception:
                logger.exception("Failed to start continuation #%s of job #%s", child_id, parent_id)
