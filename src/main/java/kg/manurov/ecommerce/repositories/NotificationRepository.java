package kg.manurov.ecommerce.repositories;

import kg.manurov.ecommerce.models.Notification;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface NotificationRepository extends MongoRepository<Notification, String> {
}
