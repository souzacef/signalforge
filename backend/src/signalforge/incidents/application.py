from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from signalforge.incidents import service
from signalforge.incidents.events import IncidentCreated, IncidentCreatedPayload
from signalforge.incidents.schemas import IncidentCreate, IncidentResponse
from signalforge.outbox.models import OutboxEvent


async def create_incident_with_event(
    session: AsyncSession,
    incident_data: IncidentCreate,
) -> IncidentResponse:
    """Commit creation and its event together; the caller scopes session cleanup."""
    incident = await service.create_incident(session, incident_data)
    event = IncidentCreated(
        event_id=uuid4(),
        occurred_at=incident.created_at,
        aggregate_id=incident.id,
        payload=IncidentCreatedPayload(
            source=incident.source,
            title=incident.title,
            description=incident.description,
            severity=incident.severity,
            incident_occurred_at=incident.occurred_at,
        ),
    )
    session.add(
        OutboxEvent(
            id=event.event_id,
            event_type=event.event_type,
            event_version=event.event_version,
            aggregate_id=event.aggregate_id,
            payload=event.payload.model_dump(mode="json"),
            occurred_at=event.occurred_at,
        )
    )
    await session.flush()
    response = IncidentResponse.model_validate(incident)

    # Authentication has already autobegun this request's shared transaction.
    await session.commit()
    return response
